package controller

import (
	"context"
	"strings"
	"sync"
	"testing"

	loops "go.miloapis.com/email-provider-loops/pkg/loops"
	notificationv1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/finalizer"
)

// recordingLoops is a loops.API that records every mailing list addition and succeeds. A manager calls it from
// its own goroutines, so reads go through snapshot.
type recordingLoops struct {
	mu    sync.Mutex
	added []string
}

func (l *recordingLoops) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.added...)
}

func (l *recordingLoops) UpsertContact(context.Context, loops.ContactRequest) (*loops.APIResponse, error) {
	return &loops.APIResponse{Success: true}, nil
}

func (l *recordingLoops) DeleteContact(context.Context, string) (*loops.APIResponse, error) {
	return &loops.APIResponse{Success: true}, nil
}

func (l *recordingLoops) AddToMailingList(_ context.Context, userID, listID string) (*loops.APIResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.added = append(l.added, userID+"/"+listID)
	return &loops.APIResponse{Success: true}, nil
}

func (l *recordingLoops) RemoveFromMailingList(context.Context, string, string) (*loops.APIResponse, error) {
	return &loops.APIResponse{Success: true}, nil
}

func membershipFixtures(providers []notificationv1.ContactGroupProviderSpec, cond *metav1.Condition) []client.Object {
	contact := &notificationv1.Contact{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default", UID: types.UID("contact-uid")},
		Spec:       notificationv1.ContactSpec{GivenName: "Alice", FamilyName: "Doe", Email: "alice@example.com"},
	}
	group := &notificationv1.ContactGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "devs", Namespace: "default"},
		Spec:       notificationv1.ContactGroupSpec{DisplayName: "Devs", Providers: providers},
	}
	membership := &notificationv1.ContactGroupMembership{
		ObjectMeta: metav1.ObjectMeta{
			Name: "m-1", Namespace: "default",
			// The finalizer is already present, so Reconcile goes past the finalizer step to the mailing list.
			Finalizers: []string{loopsContactGroupMembershipFinalizerKey},
		},
		Spec: notificationv1.ContactGroupMembershipSpec{
			ContactRef:      notificationv1.ContactReference{Name: "alice", Namespace: "default"},
			ContactGroupRef: notificationv1.ContactGroupReference{Name: "devs", Namespace: "default"},
		},
	}
	if cond != nil {
		membership.Status.Conditions = []metav1.Condition{*cond}
	}
	return []client.Object{contact, group, membership}
}

func newMembershipController(t *testing.T, api loops.API, objs ...client.Object) (*LoopsContactGroupMembershipController, client.Client) {
	t.Helper()
	sch := scheme.Scheme
	if err := notificationv1.AddToScheme(sch); err != nil {
		t.Fatalf("failed to register notification types in scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(sch).WithObjects(objs...).
		WithIndex(&notificationv1.ContactGroupMembership{}, membershipsByGroupIndex, membershipGroupKey).
		WithStatusSubresource(&notificationv1.ContactGroupMembership{}).Build()
	fs := finalizer.NewFinalizers()
	if err := fs.Register(loopsContactGroupMembershipFinalizerKey, &loopsContactGroupMembershipFinalizer{Client: c, Loops: api}); err != nil {
		t.Fatalf("failed to register finalizer: %v", err)
	}
	return &LoopsContactGroupMembershipController{Client: c, Loops: api, Finalizers: fs}, c
}

func loopsReady(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	cgm := &notificationv1.ContactGroupMembership{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "m-1", Namespace: "default"}, cgm); err != nil {
		t.Fatalf("failed to get membership: %v", err)
	}
	return meta.FindStatusCondition(cgm.Status.Conditions, LoopsContactGroupMembershipReadyCondition)
}

func reconcileMembership(t *testing.T, r *LoopsContactGroupMembershipController) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "m-1", Namespace: "default"}})
	return err
}

// The failure every membership of a group that Loops does not serve carried before #19.
var staleNotCreated = &metav1.Condition{
	Type:               LoopsContactGroupMembershipReadyCondition,
	Status:             metav1.ConditionFalse,
	Reason:             LoopsContactGroupMembershipNotCreatedReason,
	Message:            "Loops contact group membership not created on email provider: failed to get Loops mailing list ID: mailing list ID not found for contact group",
	LastTransitionTime: metav1.Now(),
}

func TestReconcile_GroupWithoutLoopsIsSkipped(t *testing.T) {
	for name, cond := range map[string]*metav1.Condition{"new membership": nil, "membership that failed before": staleNotCreated} {
		t.Run(name, func(t *testing.T) {
			api := &recordingLoops{}
			r, c := newMembershipController(t, api, membershipFixtures(nil, cond)...)

			if err := reconcileMembership(t, r); err != nil {
				t.Fatalf("Reconcile() returned an error for a group that Loops does not serve: %v", err)
			}
			if len(api.added) != 0 {
				t.Fatalf("Reconcile() called Loops for a group that Loops does not serve: %v", api.added)
			}
			if got := loopsReady(t, c); got != nil {
				t.Fatalf("Reconcile() left a Loops condition on a membership Loops does not serve: %+v", *got)
			}
		})
	}
}

func TestReconcile_LoopsGroupWithoutIDIsReportedOnce(t *testing.T) {
	api := &recordingLoops{}
	r, c := newMembershipController(t, api, membershipFixtures([]notificationv1.ContactGroupProviderSpec{{Name: "Loops"}}, nil)...)

	for i := 1; i <= 2; i++ {
		if err := reconcileMembership(t, r); err != nil {
			t.Fatalf("Reconcile() #%d returned an error, so the membership would retry for ever: %v", i, err)
		}
	}
	if len(api.added) != 0 {
		t.Fatalf("Reconcile() called Loops without a mailing list ID: %v", api.added)
	}
	got := loopsReady(t, c)
	if got == nil || got.Status != metav1.ConditionFalse || got.Reason != LoopsContactGroupMembershipNotCreatedReason {
		t.Fatalf("Reconcile() did not report the missing mailing list ID on the status: %+v", got)
	}
	if !strings.Contains(got.Message, "no mailing list ID") {
		t.Fatalf("the status message does not name the missing mailing list ID: %q", got.Message)
	}
}

func TestReconcile_LoopsGroupWithIDAddsTheContact(t *testing.T) {
	api := &recordingLoops{}
	r, c := newMembershipController(t, api, membershipFixtures([]notificationv1.ContactGroupProviderSpec{{Name: "Loops", ID: "list-1"}}, nil)...)

	if err := reconcileMembership(t, r); err != nil {
		t.Fatalf("Reconcile() returned an error: %v", err)
	}
	if len(api.added) != 1 || api.added[0] != "contact-uid/list-1" {
		t.Fatalf("Reconcile() should add the contact to list-1 once, got %v", api.added)
	}
	if got := loopsReady(t, c); got == nil || got.Status != metav1.ConditionTrue {
		t.Fatalf("Reconcile() did not mark the membership created: %+v", got)
	}
}

func TestReconcile_CreatedMembershipIsLeftAlone(t *testing.T) {
	created := &metav1.Condition{
		Type:               LoopsContactGroupMembershipReadyCondition,
		Status:             metav1.ConditionTrue,
		Reason:             LoopsContactGroupMembershipCreatedReason,
		Message:            "Loops contact group membership created on email provider",
		LastTransitionTime: metav1.Now(),
	}
	api := &recordingLoops{}
	r, c := newMembershipController(t, api, membershipFixtures(nil, created)...)

	if err := reconcileMembership(t, r); err != nil {
		t.Fatalf("Reconcile() returned an error: %v", err)
	}
	if len(api.added) != 0 {
		t.Fatalf("Reconcile() called Loops for a membership already created: %v", api.added)
	}
	if got := loopsReady(t, c); got == nil || got.Reason != LoopsContactGroupMembershipCreatedReason {
		t.Fatalf("Reconcile() changed a created membership's condition: %+v", got)
	}
}
