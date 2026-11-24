package controller

import (
	"context"
	"strings"
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
)

// listingLoops answers the list of mailing lists with lists, or with err when it is set.
type listingLoops struct {
	lists []loops.MailingList
	err   error
}

func (l *listingLoops) ListMailingLists(context.Context) ([]loops.MailingList, error) {
	return l.lists, l.err
}

// The mailing lists the Loops account holds in these tests.
var accountLists = []loops.MailingList{{ID: "list-1", Name: "Product updates"}, {ID: "list-2", Name: "Internal"}}

func groupWith(providers ...notificationv1.ContactGroupProviderSpec) *notificationv1.ContactGroup {
	return &notificationv1.ContactGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "devs", Namespace: "default"},
		Spec:       notificationv1.ContactGroupSpec{DisplayName: "Devs", Providers: providers},
	}
}

func newGroupController(t *testing.T, api *listingLoops, group *notificationv1.ContactGroup) (*LoopsContactGroupController, client.Client) {
	t.Helper()
	sch := scheme.Scheme
	if err := notificationv1.AddToScheme(sch); err != nil {
		t.Fatalf("failed to register notification types in scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(sch).WithObjects(group).
		WithStatusSubresource(&notificationv1.ContactGroup{}).Build()
	return &LoopsContactGroupController{Client: c, Loops: api}, c
}

func reconcileGroup(t *testing.T, r *LoopsContactGroupController) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "devs", Namespace: "default"}})
	return err
}

func groupLoopsReady(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	cg := &notificationv1.ContactGroup{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "devs", Namespace: "default"}, cg); err != nil {
		t.Fatalf("failed to get contact group: %v", err)
	}
	return meta.FindStatusCondition(cg.Status.Conditions, LoopsContactGroupReadyCondition)
}

func TestReconcileGroup_UnknownMailingListIsReported(t *testing.T) {
	r, c := newGroupController(t, &listingLoops{lists: accountLists}, groupWith(notificationv1.ContactGroupProviderSpec{Name: "Loops", ID: "list-gone"}))

	if err := reconcileGroup(t, r); err != nil {
		t.Fatalf("Reconcile() returned an error, so the group would retry for ever: %v", err)
	}
	got := groupLoopsReady(t, c)
	if got == nil || got.Status != metav1.ConditionFalse || got.Reason != LoopsMailingListUnknownReason {
		t.Fatalf("Reconcile() did not report the unknown mailing list on the group: %+v", got)
	}
	if !strings.Contains(got.Message, `"list-gone"`) {
		t.Fatalf("the status message does not name the unknown mailing list: %q", got.Message)
	}
}

func TestReconcileGroup_LoopsWithoutIDIsReported(t *testing.T) {
	r, c := newGroupController(t, &listingLoops{lists: accountLists}, groupWith(notificationv1.ContactGroupProviderSpec{Name: "Loops"}))

	if err := reconcileGroup(t, r); err != nil {
		t.Fatalf("Reconcile() returned an error, so the group would retry for ever: %v", err)
	}
	got := groupLoopsReady(t, c)
	if got == nil || got.Status != metav1.ConditionFalse || got.Reason != LoopsMailingListIDMissingReason {
		t.Fatalf("Reconcile() did not report the missing mailing list ID on the group: %+v", got)
	}
}

func TestReconcileGroup_GroupWithoutLoopsGetsNoCondition(t *testing.T) {
	r, c := newGroupController(t, &listingLoops{lists: accountLists}, groupWith())

	if err := reconcileGroup(t, r); err != nil {
		t.Fatalf("Reconcile() returned an error for a group that Loops does not serve: %v", err)
	}
	if got := groupLoopsReady(t, c); got != nil {
		t.Fatalf("Reconcile() set a Loops condition on a group that Loops does not serve: %+v", *got)
	}
}

func TestReconcileGroup_ListedMailingListIsReady(t *testing.T) {
	r, c := newGroupController(t, &listingLoops{lists: accountLists}, groupWith(notificationv1.ContactGroupProviderSpec{Name: "Loops", ID: "list-2"}))

	if err := reconcileGroup(t, r); err != nil {
		t.Fatalf("Reconcile() returned an error: %v", err)
	}
	if got := groupLoopsReady(t, c); got == nil || got.Status != metav1.ConditionTrue || got.Reason != LoopsMailingListFoundReason {
		t.Fatalf("Reconcile() did not mark the group ready for a mailing list Loops has: %+v", got)
	}
}
