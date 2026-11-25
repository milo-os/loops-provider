package controller

import (
	"context"
	"testing"
	"time"

	notificationv1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// startGroupManager runs the contact group controller in a manager against a fresh API server, with a Loops account
// that has accountLists, and returns a client of that server. Each test has its own manager, so the controller's name
// is used once per manager, not once per process.
func startGroupManager(t *testing.T) client.Client {
	t.Helper()
	cfg, sch := startAPIServer(t)
	skipNameValidation := true
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: sch, Metrics: metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: &skipNameValidation}})
	if err != nil {
		t.Fatalf("create the manager: %v", err)
	}
	if err := (&LoopsContactGroupController{Client: mgr.GetClient(), Loops: &listingLoops{lists: accountLists}}).SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(ctx) }()

	c, err := client.New(cfg, client.Options{Scheme: sch})
	if err != nil {
		t.Fatalf("create a client: %v", err)
	}
	return c
}

func TestManager_AGroupWithAnUnknownMailingListSaysSoOnItself(t *testing.T) {
	ctx := context.Background()
	c := startGroupManager(t)

	group := &notificationv1.ContactGroup{ObjectMeta: metav1.ObjectMeta{Name: "devs", Namespace: "default"},
		Spec: notificationv1.ContactGroupSpec{DisplayName: "Devs", Visibility: notificationv1.ContactGroupVisibilityPrivate,
			Providers: []notificationv1.ContactGroupProviderSpec{{Name: "Loops", ID: "list-gone"}}}}
	if err := c.Create(ctx, group); err != nil {
		t.Fatalf("create the group: %v", err)
	}

	var got *metav1.Condition
	if !eventually(20*time.Second, func() bool {
		if c.Get(ctx, client.ObjectKeyFromObject(group), group) != nil {
			return false
		}
		got = meta.FindStatusCondition(group.Status.Conditions, LoopsContactGroupReadyCondition)
		return got != nil
	}) {
		t.Fatal("20 s after the group was created, it had no Loops condition")
	}
	if got.Status != metav1.ConditionFalse || got.Reason != LoopsMailingListUnknownReason {
		t.Fatalf("the group's Loops condition does not report the unknown mailing list: %+v", *got)
	}
}

func TestManager_AGroupThatDoesNotListLoopsLosesAnOldLoopsCondition(t *testing.T) {
	ctx := context.Background()
	c := startGroupManager(t)

	// Milo refuses a spec change that removes a provider from a group, so the old condition is written on a group
	// that does not list Loops.
	group := &notificationv1.ContactGroup{ObjectMeta: metav1.ObjectMeta{Name: "devs", Namespace: "default"},
		Spec: notificationv1.ContactGroupSpec{DisplayName: "Devs", Visibility: notificationv1.ContactGroupVisibilityPrivate}}
	if err := c.Create(ctx, group); err != nil {
		t.Fatalf("create the group: %v", err)
	}
	group.Status.Conditions = []metav1.Condition{
		{Type: LoopsContactGroupReadyCondition, Status: metav1.ConditionTrue, Reason: LoopsMailingListFoundReason,
			Message: `Loops has mailing list "list-1"`, LastTransitionTime: metav1.Now()},
		{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Reconciled", LastTransitionTime: metav1.Now()},
	}
	if err := c.Status().Update(ctx, group); err != nil {
		t.Fatalf("write the old Loops condition: %v", err)
	}

	if !eventually(20*time.Second, func() bool {
		if c.Get(ctx, client.ObjectKeyFromObject(group), group) != nil {
			return false
		}
		return meta.FindStatusCondition(group.Status.Conditions, LoopsContactGroupReadyCondition) == nil
	}) {
		t.Fatalf("20 s after a group that does not list Loops got a Loops condition, it still had it: %+v", group.Status.Conditions)
	}
	ready := meta.FindStatusCondition(group.Status.Conditions, "Ready")
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != "Reconciled" {
		t.Fatalf("removing the Loops condition changed the group's other conditions: %+v", group.Status.Conditions)
	}
}
