package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	loops "go.miloapis.com/email-provider-loops/pkg/loops"
	notificationv1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/finalizer"
)

// upsertRecorder is a Loops API server that records the JSON body of every contact upsert and succeeds.
type upsertRecorder struct {
	mu     sync.Mutex
	bodies []map[string]any
}

func (u *upsertRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	u.mu.Lock()
	u.bodies = append(u.bodies, body)
	u.mu.Unlock()
	_, _ = w.Write([]byte(`{"success":true,"id":"op-1"}`))
}

// reconcileContactAgainstLoops reconciles one Contact with the real SDK client, so the test reads the body as Loops
// receives it.
func reconcileContactAgainstLoops(t *testing.T, contact *notificationv1.Contact) []map[string]any {
	t.Helper()
	recorder := &upsertRecorder{}
	server := httptest.NewServer(recorder)
	t.Cleanup(server.Close)
	api, err := loops.NewSDK("test-key", loops.WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("failed to create Loops client: %v", err)
	}

	sch := scheme.Scheme
	if err := notificationv1.AddToScheme(sch); err != nil {
		t.Fatalf("failed to register notification types in scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(sch).WithObjects(contact).
		WithStatusSubresource(&notificationv1.Contact{}).Build()
	fs := finalizer.NewFinalizers()
	if err := fs.Register(loopsContactFinalizerKey, &loopsContactFinalizer{Client: c, Loops: api}); err != nil {
		t.Fatalf("failed to register finalizer: %v", err)
	}
	r := &LoopsContactController{Client: c, Loops: api, Finalizers: fs}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: contact.Name, Namespace: contact.Namespace}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	return recorder.bodies
}

func contactAtGeneration(generation int64, conditions ...metav1.Condition) *notificationv1.Contact {
	return &notificationv1.Contact{
		ObjectMeta: metav1.ObjectMeta{
			Name: "alice", Namespace: "default", UID: types.UID("contact-uid"), Generation: generation,
			// The finalizer is already present, so Reconcile goes past the finalizer step to the upsert.
			Finalizers: []string{loopsContactFinalizerKey},
		},
		Spec:   notificationv1.ContactSpec{GivenName: "Alice", FamilyName: "Doe", Email: "alice@example.com"},
		Status: notificationv1.ContactStatus{Conditions: conditions},
	}
}

// The upsert never sends `subscribed`. Loops subscribes a new contact by default, and the field on an update
// re-subscribes a person who unsubscribed in Loops.
func TestReconcile_UpsertNeverSendsSubscribed(t *testing.T) {
	readyAtGeneration1 := metav1.Condition{
		Type:               LoopsContactReadyCondition,
		Status:             metav1.ConditionTrue,
		Reason:             LoopsContactCreatedReason,
		ObservedGeneration: 1,
		LastTransitionTime: metav1.Now(),
	}
	cases := map[string]*notificationv1.Contact{
		"creation":    contactAtGeneration(1),
		"spec change": contactAtGeneration(2, readyAtGeneration1),
	}
	for name, contact := range cases {
		t.Run(name, func(t *testing.T) {
			bodies := reconcileContactAgainstLoops(t, contact)
			if len(bodies) != 1 {
				t.Fatalf("expected one upsert, got %d", len(bodies))
			}
			if _, sent := bodies[0]["subscribed"]; sent {
				t.Errorf("upsert body carries subscribed: %v", bodies[0])
			}
			if bodies[0]["userId"] != "contact-uid" || bodies[0]["email"] != "alice@example.com" {
				t.Errorf("upsert body lost the contact's identity: %v", bodies[0])
			}
		})
	}
}
