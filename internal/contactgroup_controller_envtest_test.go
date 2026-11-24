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
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func TestManager_AGroupWithAnUnknownMailingListSaysSoOnItself(t *testing.T) {
	ctx := context.Background()
	cfg, sch := startAPIServer(t)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: sch, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatalf("create the manager: %v", err)
	}
	if err := (&LoopsContactGroupController{Client: mgr.GetClient(), Loops: &listingLoops{lists: accountLists}}).SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}
	mgrCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(mgrCtx) }()
	c, err := client.New(cfg, client.Options{Scheme: sch})
	if err != nil {
		t.Fatalf("create a client: %v", err)
	}

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
