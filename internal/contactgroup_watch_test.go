package controller

import (
	"context"
	"reflect"
	"testing"

	notificationv1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func membershipOf(name, namespace, group, groupNamespace string) *notificationv1.ContactGroupMembership {
	return &notificationv1.ContactGroupMembership{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: notificationv1.ContactGroupMembershipSpec{
			ContactRef:      notificationv1.ContactReference{Name: "alice", Namespace: "default"},
			ContactGroupRef: notificationv1.ContactGroupReference{Name: group, Namespace: groupNamespace},
		},
	}
}

func TestMembershipsOfGroup_EnqueuesEveryMembershipOfThatGroupOnly(t *testing.T) {
	group := &notificationv1.ContactGroup{ObjectMeta: metav1.ObjectMeta{Name: "devs", Namespace: "default"}}
	r, _ := newMembershipController(t, &recordingLoops{},
		group,
		membershipOf("m-1", "default", "devs", "default"),
		membershipOf("m-2", "team", "devs", "default"),
		membershipOf("m-3", "default", "ops", "default"),
		membershipOf("m-4", "default", "devs", "other"),
	)

	want := []reconcile.Request{
		{NamespacedName: types.NamespacedName{Name: "m-1", Namespace: "default"}},
		{NamespacedName: types.NamespacedName{Name: "m-2", Namespace: "team"}},
	}
	if got := r.membershipsOfGroup(context.Background(), group); !reflect.DeepEqual(got, want) {
		t.Fatalf("membershipsOfGroup() = %v, want the two memberships of default/devs, from any namespace, and none of ops or of other/devs", got)
	}
}

func TestGroupSpecChanged_PassesOnlyASpecUpdate(t *testing.T) {
	at := func(generation int64) *notificationv1.ContactGroup {
		return &notificationv1.ContactGroup{ObjectMeta: metav1.ObjectMeta{Name: "devs", Namespace: "default", Generation: generation}}
	}
	p := groupSpecChanged()
	cases := map[string]bool{
		"an update that changes the spec": p.Update(event.UpdateEvent{ObjectOld: at(1), ObjectNew: at(2)}),
		"an update of status or metadata": p.Update(event.UpdateEvent{ObjectOld: at(2), ObjectNew: at(2)}),
		"a create":                        p.Create(event.CreateEvent{Object: at(1)}),
		"a delete":                        p.Delete(event.DeleteEvent{Object: at(1)}),
		"a generic event":                 p.Generic(event.GenericEvent{Object: at(1)}),
	}
	for name, passed := range cases {
		if want := name == "an update that changes the spec"; passed != want {
			t.Errorf("%s: passed = %v, want %v", name, passed, want)
		}
	}
}

func TestAGroupThatAddsLoopsReachesItsMemberships(t *testing.T) {
	ctx := context.Background()
	api := &recordingLoops{}
	r, c := newMembershipController(t, api, membershipFixtures(nil, nil)...)
	if err := reconcileMembership(t, r); err != nil {
		t.Fatalf("Reconcile() before the group lists Loops: %v", err)
	}
	if len(api.added) != 0 {
		t.Fatalf("Reconcile() called Loops before the group listed it: %v", api.added)
	}

	group := &notificationv1.ContactGroup{}
	if err := c.Get(ctx, client.ObjectKey{Name: "devs", Namespace: "default"}, group); err != nil {
		t.Fatalf("get the group: %v", err)
	}
	group.Spec.Providers = []notificationv1.ContactGroupProviderSpec{{Name: "Loops", ID: "list-1"}}
	if err := c.Update(ctx, group); err != nil {
		t.Fatalf("add Loops to the group: %v", err)
	}

	for _, req := range r.membershipsOfGroup(ctx, group) {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile(%s) after the group added Loops: %v", req, err)
		}
	}
	if !reflect.DeepEqual(api.added, []string{"contact-uid/list-1"}) {
		t.Fatalf("the group's membership was not added to its new Loops list: %v", api.added)
	}
}
