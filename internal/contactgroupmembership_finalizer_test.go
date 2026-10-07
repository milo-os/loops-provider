package controller

import (
	"context"
	"testing"

	notificationv1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	finalizerpkg "sigs.k8s.io/controller-runtime/pkg/finalizer"
)

// newMembershipFinalizerTestClient returns a fake client holding the given
// objects. The scheme is always enriched with the notification API types.
func newMembershipFinalizerTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	sch := scheme.Scheme
	if err := notificationv1.AddToScheme(sch); err != nil {
		t.Fatalf("failed to register notification types in scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(objs...).
		Build()
}

func TestLoopsContactGroupMembershipFinalizer_ReferencedContactMissing(t *testing.T) {
	ctx := context.Background()

	// The referenced Contact and ContactGroup are NOT present in the client,
	// simulating a teardown where they were already deleted. The finalizer must
	// complete rather than hard-failing (which would deadlock the membership
	// and, transitively, the parent ContactGroup deletion).
	membership := &notificationv1.ContactGroupMembership{
		ObjectMeta: metav1.ObjectMeta{Name: "m-1", Namespace: "default"},
		Spec: notificationv1.ContactGroupMembershipSpec{
			ContactRef:      notificationv1.ContactReference{Name: "alice", Namespace: "default"},
			ContactGroupRef: notificationv1.ContactGroupReference{Name: "devs", Namespace: "default"},
		},
	}

	c := newMembershipFinalizerTestClient(t)
	f := &loopsContactGroupMembershipFinalizer{Client: c}

	res, err := f.Finalize(ctx, membership)
	if err != nil {
		t.Fatalf("Finalize() returned error when referenced Contact is missing: %v", err)
	}
	if res != (finalizerpkg.Result{}) {
		t.Fatalf("Finalize() expected empty result, got %+v", res)
	}
}

func TestLoopsContactGroupMembershipFinalizer_ReferencedContactGroupMissing(t *testing.T) {
	ctx := context.Background()

	// Same as above but only the referenced ContactGroup is missing.
	contact := &notificationv1.Contact{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default"},
		Spec:       notificationv1.ContactSpec{FamilyName: "Doe", GivenName: "Alice", Email: "alice@example.com"},
	}
	membership := &notificationv1.ContactGroupMembership{
		ObjectMeta: metav1.ObjectMeta{Name: "m-1", Namespace: "default"},
		Spec: notificationv1.ContactGroupMembershipSpec{
			ContactRef:      notificationv1.ContactReference{Name: "alice", Namespace: "default"},
			ContactGroupRef: notificationv1.ContactGroupReference{Name: "devs", Namespace: "default"},
		},
	}

	c := newMembershipFinalizerTestClient(t, contact)
	f := &loopsContactGroupMembershipFinalizer{Client: c}

	res, err := f.Finalize(ctx, membership)
	if err != nil {
		t.Fatalf("Finalize() returned error when referenced ContactGroup is missing: %v", err)
	}
	if res != (finalizerpkg.Result{}) {
		t.Fatalf("Finalize() expected empty result, got %+v", res)
	}
}

func TestLoopsContactGroupMembershipFinalizer_NoMailingListConfigured(t *testing.T) {
	ctx := context.Background()

	// Both referenced resources exist, but the ContactGroup has no Loops
	// mailing list ID configured, so no Loops-side membership can exist. The
	// finalizer must complete (via errMailingListIDNotFound) rather than
	// calling the Loops API and failing.
	contact := &notificationv1.Contact{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default"},
		Spec:       notificationv1.ContactSpec{FamilyName: "Doe", GivenName: "Alice", Email: "alice@example.com"},
	}
	contactGroup := &notificationv1.ContactGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "devs", Namespace: "default"},
		Spec:       notificationv1.ContactGroupSpec{DisplayName: "Developers"},
	}
	membership := &notificationv1.ContactGroupMembership{
		ObjectMeta: metav1.ObjectMeta{Name: "m-1", Namespace: "default"},
		Spec: notificationv1.ContactGroupMembershipSpec{
			ContactRef:      notificationv1.ContactReference{Name: "alice", Namespace: "default"},
			ContactGroupRef: notificationv1.ContactGroupReference{Name: "devs", Namespace: "default"},
		},
	}

	c := newMembershipFinalizerTestClient(t, contact, contactGroup)
	f := &loopsContactGroupMembershipFinalizer{Client: c}

	res, err := f.Finalize(ctx, membership)
	if err != nil {
		t.Fatalf("Finalize() returned error when no Loops mailing list is configured: %v", err)
	}
	if res != (finalizerpkg.Result{}) {
		t.Fatalf("Finalize() expected empty result, got %+v", res)
	}
}