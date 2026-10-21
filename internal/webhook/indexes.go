package webhook

import (
	"context"
	"fmt"

	notificationmiloapiscomv1alpha1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	contactStatusProviderIDIndexKey = "contact-status-providerID"
	groupProviderIDIndexKey         = "group-providerID"
	groupMembershipRemovalIndexKey  = "group-membership-removal"
	groupMembershipIndexKey         = "group-membership"
)

// fieldIndex is a cache index the handler looks objects up by.
type fieldIndex struct {
	object  client.Object
	field   string
	extract client.IndexerFunc
}

// fieldIndexes are every index the handler reads. Loops names a contact by the userId the contact controller sends,
// which is the Contact's UID, and a mailing list by the ID in its group's Loops provider entry. A membership and a
// Removal are found by their contact and group.
var fieldIndexes = []fieldIndex{
	{&notificationmiloapiscomv1alpha1.Contact{}, contactStatusProviderIDIndexKey, contactUID},
	{&notificationmiloapiscomv1alpha1.ContactGroup{}, groupProviderIDIndexKey, groupLoopsListID},
	{&notificationmiloapiscomv1alpha1.ContactGroupMembershipRemoval{}, groupMembershipRemovalIndexKey, removalTuple},
	{&notificationmiloapiscomv1alpha1.ContactGroupMembership{}, groupMembershipIndexKey, membershipTuple},
}

func contactUID(rawObj client.Object) []string {
	contact := rawObj.(*notificationmiloapiscomv1alpha1.Contact)
	if contact.UID == "" {
		return nil
	}
	return []string{string(contact.UID)}
}

func groupLoopsListID(rawObj client.Object) []string {
	group := rawObj.(*notificationmiloapiscomv1alpha1.ContactGroup)
	for _, provider := range group.Spec.Providers {
		if provider.Name == "Loops" {
			return []string{provider.ID}
		}
	}
	return nil
}

func removalTuple(rawObj client.Object) []string {
	removal := rawObj.(*notificationmiloapiscomv1alpha1.ContactGroupMembershipRemoval)
	return []string{contactGroupKey(removal.Spec.ContactRef, removal.Spec.ContactGroupRef)}
}

func membershipTuple(rawObj client.Object) []string {
	membership := rawObj.(*notificationmiloapiscomv1alpha1.ContactGroupMembership)
	return []string{contactGroupKey(membership.Spec.ContactRef, membership.Spec.ContactGroupRef)}
}

func contactGroupKey(
	contactRef notificationmiloapiscomv1alpha1.ContactReference,
	groupRef notificationmiloapiscomv1alpha1.ContactGroupReference,
) string {
	return fmt.Sprintf("%s-%s-%s-%s", contactRef.Name, contactRef.Namespace, groupRef.Name, groupRef.Namespace)
}

// setupIndexes sets up the required field indexes for webhook operations
func setupIndexes(mgr ctrl.Manager) error {
	for _, index := range fieldIndexes {
		if err := mgr.GetFieldIndexer().IndexField(context.Background(), index.object, index.field, index.extract); err != nil {
			return fmt.Errorf("failed to create index %s: %w", index.field, err)
		}
	}
	return nil
}
