package webhook

import (
	"context"
	"fmt"

	"go.miloapis.com/email-provider-loops/internal/util"
	"go.miloapis.com/email-provider-loops/pkg/loops"
	notificationmiloapiscomv1alpha1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contacts,verbs=get;list;watch
// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contacts/status,verbs=get;patch
// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contactgroups,verbs=get;list;watch
// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contactgroupmemberships,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contactgroupmembershipremovals,verbs=get;list;watch;create;delete

// receiver applies Loops' events to Milo.
type receiver struct {
	client client.Client
}

func NewLoopsContactGroupMembershipWebhookV1(k8sClient client.Client, signingSecret string) *Webhook {
	rc := &receiver{client: k8sClient}
	return &Webhook{
		Handler:       HandlerFunc(rc.handle),
		Endpoint:      "/apis/emailnotification.k8s.io/v1/loops/contactgroupmemberships",
		signingSecret: signingSecret,
		budget:        handlerBudget,
		seen:          newSeenDeliveries(),
	}
}

// handle acts on one event that ServeHTTP has verified and parsed.
func (rc *receiver) handle(ctx context.Context, req Request) Response {
	contact, miss := rc.contactOf(ctx, req.BaseEvent.ContactIdentity.UserID)
	if contact == nil {
		return miss
	}
	switch req.BaseEvent.EventName {
	case loops.EventNameContactDeleted:
		return rc.contactDeleted(ctx, contact, req.BaseEvent.EventTime)
	case loops.EventNameContactUnsubscribed:
		return rc.recordAudienceState(ctx, contact, unsubscribed(req.BaseEvent.EventTime))
	case loops.EventNameEmailResubscribed:
		return rc.recordAudienceState(ctx, contact, resubscribed(req.BaseEvent.EventTime))
	}

	group, miss := rc.groupOf(ctx, mailingListID(req))
	if group == nil {
		return miss
	}
	if req.MailingListSubscribedEvent != nil {
		return rc.listSubscribed(ctx, contact, group, req.BaseEvent.EventTime)
	}
	return rc.listUnsubscribed(ctx, contact, group)
}

func mailingListID(req Request) string {
	if req.MailingListSubscribedEvent != nil {
		return req.MailingListSubscribedEvent.MailingList.ID
	}
	if req.MailingListUnsubscribedEvent != nil {
		return req.MailingListUnsubscribedEvent.MailingList.ID
	}
	return ""
}

// contactOf returns the Contact Loops names, or the answer when there is none to act on.
func (rc *receiver) contactOf(ctx context.Context, userID string) (*notificationmiloapiscomv1alpha1.Contact, Response) {
	if userID == "" {
		return nil, handledResponse(outcomeNoUserID)
	}
	contact, err := getContactByProviderID(ctx, rc.client, userID)
	if err != nil {
		return nil, failed(ctx, err, "Failed to get contact by user UID")
	}
	if contact == nil {
		return nil, handledResponse(outcomeUnknownContact)
	}
	return contact, Response{}
}

// groupOf returns the ContactGroup of a Loops mailing list, or the answer when there is none to act on.
func (rc *receiver) groupOf(ctx context.Context, listID string) (*notificationmiloapiscomv1alpha1.ContactGroup, Response) {
	if listID == "" {
		return nil, handledResponse(outcomeNoListID)
	}
	group, err := getContactGroupByProviderID(ctx, rc.client, listID)
	if err != nil {
		return nil, failed(ctx, err, "Failed to get contact group by group ID")
	}
	if group == nil {
		return nil, handledResponse(outcomeUnknownList)
	}
	return group, Response{}
}

// listSubscribed undoes the person's opt-out only when Loops saw the subscribe after it. Loops' eventTime and Milo's
// creationTimestamp both have whole seconds, and a tie goes to the opt-out. A retry carries the event's first time, so
// it never outranks a later opt-out.
func (rc *receiver) listSubscribed(
	ctx context.Context,
	contact *notificationmiloapiscomv1alpha1.Contact,
	group *notificationmiloapiscomv1alpha1.ContactGroup,
	eventTime int64,
) Response {
	removal, err := getContactGroupMembershipRemoval(ctx, rc.client, contact, group)
	if err != nil {
		return failed(ctx, err, "Failed to get contact group membership removal")
	}
	if removal != nil {
		if eventTime <= removal.CreationTimestamp.Unix() {
			return handledResponse(outcomeOlderThanOptOut)
		}
		if err := rc.client.Delete(ctx, removal); err != nil && !apierrors.IsNotFound(err) {
			return failed(ctx, err, "Failed to delete contact group membership removal")
		}
	}
	return rc.ensureMembership(ctx, contact, group)
}

// ensureMembership adds the person to the group. Our own add comes back from Loops as a subscribe, so a membership
// that exists, or Milo's refusal of a second one, is the usual case and not a failure.
func (rc *receiver) ensureMembership(
	ctx context.Context,
	contact *notificationmiloapiscomv1alpha1.Contact,
	group *notificationmiloapiscomv1alpha1.ContactGroup,
) Response {
	membership, err := getContactGroupMembership(ctx, rc.client, contact, group)
	if err != nil {
		return failed(ctx, err, "Failed to get contact group membership")
	}
	if membership != nil {
		return handledResponse(outcomeMemberExists)
	}
	if err := createContactGroupMembership(ctx, rc.client, contact, group); err != nil {
		if _, duplicate := util.DuplicateRefusal(err); duplicate || apierrors.IsAlreadyExists(err) {
			return handledResponse(outcomeMemberExists)
		}
		return failed(ctx, err, "Failed to create contact group membership")
	}
	return OkResponse()
}

// listUnsubscribed records the person's opt-out. A person's own unsubscribe always meets a live membership; without
// one, or with one Milo is deleting, the event is the echo of our own removal from the list.
func (rc *receiver) listUnsubscribed(
	ctx context.Context,
	contact *notificationmiloapiscomv1alpha1.Contact,
	group *notificationmiloapiscomv1alpha1.ContactGroup,
) Response {
	membership, err := getContactGroupMembership(ctx, rc.client, contact, group)
	if err != nil {
		return failed(ctx, err, "Failed to get contact group membership")
	}
	if membership == nil || !membership.DeletionTimestamp.IsZero() {
		return handledResponse(outcomeEcho)
	}

	removal, err := getContactGroupMembershipRemoval(ctx, rc.client, contact, group)
	if err != nil {
		return failed(ctx, err, "Failed to get contact group membership removal")
	}
	if removal != nil {
		return handledResponse(outcomeAlreadyRemoved)
	}
	if err := createContactGroupMembershipRemoval(ctx, rc.client, contact, group); err != nil {
		if _, duplicate := util.DuplicateRefusal(err); duplicate {
			return handledResponse(outcomeAlreadyRemoved)
		}
		return failed(ctx, err, "Failed to create contact group membership removal")
	}
	return OkResponse()
}

// failed logs a Milo call that failed and answers 5xx, so Loops retries the event.
func failed(ctx context.Context, err error, msg string) Response {
	logf.FromContext(ctx).Error(err, msg)
	return InternalServerErrorResponse()
}

// getContactByProviderID retrieves a Contact by the userId Loops carries, which is the Contact's UID
func getContactByProviderID(ctx context.Context, k8sClient client.Client, providerID string) (*notificationmiloapiscomv1alpha1.Contact, error) {
	log := logf.FromContext(ctx)

	var contactList notificationmiloapiscomv1alpha1.ContactList
	if err := k8sClient.List(ctx, &contactList,
		client.MatchingFields{contactStatusProviderIDIndexKey: providerID},
	); err != nil {
		return nil, err
	}

	if len(contactList.Items) == 0 {
		return nil, nil
	}

	if len(contactList.Items) > 1 {
		log.Info("Multiple contacts found with same provider ID, using first one",
			"providerID", providerID,
			"count", len(contactList.Items))
	}

	return &contactList.Items[0], nil
}

// getContactGroupByProviderID retrieves a ContactGroup by its spec.providers.loops.providerID field using the indexed field
func getContactGroupByProviderID(ctx context.Context, k8sClient client.Client, providerID string) (*notificationmiloapiscomv1alpha1.ContactGroup, error) {
	log := logf.FromContext(ctx)

	var contactGroupList notificationmiloapiscomv1alpha1.ContactGroupList
	if err := k8sClient.List(ctx, &contactGroupList,
		client.MatchingFields{groupProviderIDIndexKey: providerID},
	); err != nil {
		return nil, err
	}

	if len(contactGroupList.Items) == 0 {
		return nil, nil
	}

	if len(contactGroupList.Items) > 1 {
		log.Info("Multiple contact groups found with same provider ID, using first one",
			"providerID", providerID,
			"count", len(contactGroupList.Items))
	}

	return &contactGroupList.Items[0], nil
}

// CreateContactGroupMembership creates a ContactGroupMembership in Kubernetes
func createContactGroupMembership(ctx context.Context, k8sClient client.Client, contact *notificationmiloapiscomv1alpha1.Contact, group *notificationmiloapiscomv1alpha1.ContactGroup) error {
	log := logf.FromContext(ctx)

	contactGroupMembership := &notificationmiloapiscomv1alpha1.ContactGroupMembership{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-%s", group.Name, contact.Name),
			Namespace:    group.Namespace,
		},
		Spec: notificationmiloapiscomv1alpha1.ContactGroupMembershipSpec{
			ContactRef:      contactRefOf(contact),
			ContactGroupRef: groupRefOf(group),
		},
	}

	if err := k8sClient.Create(ctx, contactGroupMembership); err != nil {
		return err
	}

	log.Info("Created contact group membership", "contactName", contact.Name, "contactNamespace", contact.Namespace, "contactUID", contact.UID)
	return nil
}

// getContactGroupMembership retrieves the ContactGroupMembership of a contact and a group, live or being deleted
func getContactGroupMembership(ctx context.Context, k8sClient client.Client, contact *notificationmiloapiscomv1alpha1.Contact, group *notificationmiloapiscomv1alpha1.ContactGroup) (*notificationmiloapiscomv1alpha1.ContactGroupMembership, error) {
	var membershipList notificationmiloapiscomv1alpha1.ContactGroupMembershipList
	if err := k8sClient.List(ctx, &membershipList,
		client.MatchingFields{groupMembershipIndexKey: contactGroupKey(contactRefOf(contact), groupRefOf(group))},
	); err != nil {
		return nil, err
	}
	if len(membershipList.Items) == 0 {
		return nil, nil
	}
	return &membershipList.Items[0], nil
}

// GetContactGroupMembershipRemoval retrieves a ContactGroupMembershipRemoval by its spec.contactRef and spec.contactGroupRef using the indexed field
func getContactGroupMembershipRemoval(ctx context.Context, k8sClient client.Client, contact *notificationmiloapiscomv1alpha1.Contact, group *notificationmiloapiscomv1alpha1.ContactGroup) (*notificationmiloapiscomv1alpha1.ContactGroupMembershipRemoval, error) {
	log := logf.FromContext(ctx)

	var removalList notificationmiloapiscomv1alpha1.ContactGroupMembershipRemovalList
	if err := k8sClient.List(ctx, &removalList,
		client.MatchingFields{groupMembershipRemovalIndexKey: contactGroupKey(contactRefOf(contact), groupRefOf(group))},
	); err != nil {
		return nil, err
	}

	if len(removalList.Items) == 0 {
		return nil, nil
	}

	if len(removalList.Items) > 1 {
		log.Info("Multiple contact group membership removals found with same contact and group, using first one", "contactName", contact.Name, "contactNamespace", contact.Namespace, "groupName", group.Name, "groupNamespace", group.Namespace, "count", len(removalList.Items))
	}

	return &removalList.Items[0], nil
}

// CreateContactGroupMembershipRemoval creates a ContactGroupMembershipRemoval in Kubernetes
func createContactGroupMembershipRemoval(ctx context.Context, k8sClient client.Client, contact *notificationmiloapiscomv1alpha1.Contact, group *notificationmiloapiscomv1alpha1.ContactGroup) error {
	log := logf.FromContext(ctx)

	removal := &notificationmiloapiscomv1alpha1.ContactGroupMembershipRemoval{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-%s", group.Name, contact.Name),
			Namespace:    group.Namespace,
		},
		Spec: notificationmiloapiscomv1alpha1.ContactGroupMembershipRemovalSpec{
			ContactRef:      contactRefOf(contact),
			ContactGroupRef: groupRefOf(group),
		},
	}

	if err := k8sClient.Create(ctx, removal); err != nil {
		return err
	}

	log.Info("Created contact group membership removal", "removalName", removal.Name, "removalNamespace", removal.Namespace)
	return nil
}

func contactRefOf(contact *notificationmiloapiscomv1alpha1.Contact) notificationmiloapiscomv1alpha1.ContactReference {
	return notificationmiloapiscomv1alpha1.ContactReference{Name: contact.Name, Namespace: contact.Namespace}
}

func groupRefOf(group *notificationmiloapiscomv1alpha1.ContactGroup) notificationmiloapiscomv1alpha1.ContactGroupReference {
	return notificationmiloapiscomv1alpha1.ContactGroupReference{Name: group.Name, Namespace: group.Namespace}
}
