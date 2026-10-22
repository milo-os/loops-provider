package controller

import (
	"context"
	"testing"

	"go.miloapis.com/email-provider-loops/internal/util"
	notificationv1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// M7: a condition that another writer sets between our read and our write survives our write, because the write
// takes the optimistic lock and starts again from a fresh read.
func TestPatchStatusWithLock_KeepsAConditionWrittenAfterOurRead(t *testing.T) {
	cfg, sch := startAPIServer(t)
	c, err := client.New(cfg, client.Options{Scheme: sch})
	if err != nil {
		t.Fatalf("create a client: %v", err)
	}
	ctx := context.Background()
	contact := &notificationv1.Contact{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default"},
		Spec:       notificationv1.ContactSpec{GivenName: "Alice", FamilyName: "Doe", Email: "alice@example.com"},
	}
	if err := c.Create(ctx, contact); err != nil {
		t.Fatalf("create contact: %v", err)
	}

	secondWriterDone := false
	err = util.PatchStatusWithLock(ctx, c, contact, func() bool {
		if !secondWriterDone {
			secondWriterDone = true
			writeCondition(t, c, "ResendContactReady", metav1.ConditionTrue, "ContactCreated")
		}
		return meta.SetStatusCondition(&contact.Status.Conditions, metav1.Condition{
			Type: LoopsContactReadyCondition, Status: metav1.ConditionTrue, Reason: LoopsContactCreatedReason,
			LastTransitionTime: metav1.Now(),
		})
	})
	if err != nil {
		t.Fatalf("PatchStatusWithLock: %v", err)
	}

	got := &notificationv1.Contact{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(contact), got); err != nil {
		t.Fatalf("get contact: %v", err)
	}
	for _, conditionType := range []string{"ResendContactReady", LoopsContactReadyCondition} {
		if meta.FindStatusCondition(got.Status.Conditions, conditionType) == nil {
			t.Errorf("condition %s was dropped; conditions: %+v", conditionType, got.Status.Conditions)
		}
	}
}

// writeCondition is a second writer: it sets one condition on alice's status with an update of its own.
func writeCondition(t *testing.T, c client.Client, conditionType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	ctx := context.Background()
	fresh := &notificationv1.Contact{}
	if err := c.Get(ctx, client.ObjectKey{Name: "alice", Namespace: "default"}, fresh); err != nil {
		t.Fatalf("second writer: get contact: %v", err)
	}
	meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
		Type: conditionType, Status: status, Reason: reason, LastTransitionTime: metav1.Now(),
	})
	if err := c.Status().Update(ctx, fresh); err != nil {
		t.Fatalf("second writer: update status: %v", err)
	}
}
