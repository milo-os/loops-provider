package webhook

import (
	"context"
	"net/http"
	"testing"

	controller "go.miloapis.com/email-provider-loops/internal"
	"go.miloapis.com/email-provider-loops/pkg/loops"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/finalizer"
)

// upsertCounter is a loops.API that counts contact upserts and succeeds.
type upsertCounter struct {
	upserts int
}

func (u *upsertCounter) UpsertContact(context.Context, loops.ContactRequest) (*loops.APIResponse, error) {
	u.upserts++
	return &loops.APIResponse{Success: true}, nil
}

func (u *upsertCounter) DeleteContact(context.Context, string) (*loops.APIResponse, error) {
	return &loops.APIResponse{Success: true}, nil
}

func (u *upsertCounter) AddToMailingList(context.Context, string, string) (*loops.APIResponse, error) {
	return &loops.APIResponse{Success: true}, nil
}

func (u *upsertCounter) RemoveFromMailingList(context.Context, string, string) (*loops.APIResponse, error) {
	return &loops.APIResponse{Success: true}, nil
}

// M8: after contact.deleted, today's contact controller does not create the contact in Loops again.
func TestReceiver_AContactDeletedInLoopsIsNotCreatedAgain(t *testing.T) {
	wh, c := newReceiver(t, nil, alice())
	if code := deliverOnce(wh, contactEvent("contact.deleted", actAt), "msg-1"); code != http.StatusOK {
		t.Fatalf("answered %d, want 200", code)
	}

	api := &upsertCounter{}
	r := &controller.LoopsContactController{Client: c, Loops: api, Finalizers: finalizer.NewFinalizers()}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "alice", Namespace: "default"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if api.upserts != 0 {
		t.Errorf("the contact controller upserted %d times, want none", api.upserts)
	}
	if cond := contactCondition(t, c, controller.LoopsContactReadyCondition); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Errorf("LoopsContactReady = %+v, want False", cond)
	}
}
