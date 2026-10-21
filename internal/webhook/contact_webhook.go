package webhook

import (
	"context"
	"fmt"
	"net/http"
	"time"

	controller "go.miloapis.com/email-provider-loops/internal"
	"go.miloapis.com/email-provider-loops/internal/util"
	notificationmiloapiscomv1alpha1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// contactDeleted marks the Contact missing in Loops. The observed generation is the Contact's own, so the contact
// controller, which upserts only for a contact never created or a new generation, leaves the condition alone.
func (rc *receiver) contactDeleted(ctx context.Context, contact *notificationmiloapiscomv1alpha1.Contact, eventTime int64) Response {
	at := eventTimeOf(eventTime)
	condition := metav1.Condition{
		Type:               controller.LoopsContactReadyCondition,
		Status:             metav1.ConditionFalse,
		Reason:             controller.LoopsContactMissingInLoopsReason,
		Message:            fmt.Sprintf("Loops reports the contact deleted as of %s", at.UTC().Format(time.RFC3339)),
		LastTransitionTime: at,
	}
	err := util.PatchStatusWithLock(ctx, rc.client, contact, func() bool {
		condition.ObservedGeneration = contact.GetGeneration()
		return meta.SetStatusCondition(&contact.Status.Conditions, condition)
	})
	if err != nil {
		return contactWriteFailed(ctx, err)
	}
	return OkResponse()
}

// unsubscribed is Loops' contact.unsubscribed as a condition. Loops sends it when the person leaves the audience, after
// a hard bounce, and beside contact.deleted, so the message does not say which.
func unsubscribed(eventTime int64) metav1.Condition {
	at := eventTimeOf(eventTime)
	return metav1.Condition{
		Type:   controller.LoopsSubscribedCondition,
		Status: metav1.ConditionFalse,
		Reason: controller.LoopsUnsubscribedReason,
		Message: fmt.Sprintf("Loops reports the contact unsubscribed from the audience as of %s; "+
			"Loops also sends this after a hard bounce or a deletion", at.UTC().Format(time.RFC3339)),
		LastTransitionTime: at,
	}
}

// resubscribed is Loops' email.resubscribed as a condition.
func resubscribed(eventTime int64) metav1.Condition {
	at := eventTimeOf(eventTime)
	return metav1.Condition{
		Type:   controller.LoopsSubscribedCondition,
		Status: metav1.ConditionTrue,
		Reason: controller.LoopsResubscribedReason,
		Message: fmt.Sprintf("Loops reports the contact resubscribed from an email's preference center as of %s",
			at.UTC().Format(time.RFC3339)),
		LastTransitionTime: at,
	}
}

// recordAudienceState writes Loops' audience state unless the recorded state is newer: Loops can deliver events out
// of order and retries a failed one for hours.
func (rc *receiver) recordAudienceState(ctx context.Context, contact *notificationmiloapiscomv1alpha1.Contact, state metav1.Condition) Response {
	outOfOrder := false
	err := util.PatchStatusWithLock(ctx, rc.client, contact, func() bool {
		outOfOrder = !supersedes(state, meta.FindStatusCondition(contact.Status.Conditions, state.Type))
		if outOfOrder {
			return false
		}
		state.ObservedGeneration = contact.GetGeneration()
		return meta.SetStatusCondition(&contact.Status.Conditions, state)
	})
	if err != nil {
		return contactWriteFailed(ctx, err)
	}
	if outOfOrder {
		return handledResponse(outcomeOutOfOrder)
	}
	return OkResponse()
}

// supersedes reports whether next is later than current. In the same second an unsubscribe wins, as an opt-out does
// over a subscribe.
func supersedes(next metav1.Condition, current *metav1.Condition) bool {
	if current == nil || next.LastTransitionTime.After(current.LastTransitionTime.Time) {
		return true
	}
	return next.LastTransitionTime.Equal(&current.LastTransitionTime) && next.Status == metav1.ConditionFalse
}

// contactWriteFailed answers a failed Contact status write. A Contact deleted since the lookup is one Milo no longer
// holds, which needs no retry.
func contactWriteFailed(ctx context.Context, err error) Response {
	if apierrors.IsNotFound(err) {
		return handledResponse(outcomeUnknownContact)
	}
	return failed(ctx, err, "Failed to write contact status")
}

// eventTimeOf is a Loops eventTime, a Unix time in seconds, as a condition time.
func eventTimeOf(eventTime int64) metav1.Time {
	return metav1.NewTime(time.Unix(eventTime, 0))
}

// contactUnsubscribed records that the person left the whole audience, then takes them out of each group whose Loops
// list carries marketing. A private group keeps its membership; the condition records the state there.
func (rc *receiver) contactUnsubscribed(ctx context.Context, contact *notificationmiloapiscomv1alpha1.Contact, eventTime int64) Response {
	response := rc.recordAudienceState(ctx, contact, unsubscribed(eventTime))
	if response.Outcome != outcomeApplied {
		return response
	}
	return rc.leaveMarketingGroups(ctx, contact)
}

func (rc *receiver) leaveMarketingGroups(ctx context.Context, contact *notificationmiloapiscomv1alpha1.Contact) Response {
	var memberships notificationmiloapiscomv1alpha1.ContactGroupMembershipList
	if err := rc.client.List(ctx, &memberships,
		client.MatchingFields{contactMembershipIndexKey: contact.Namespace + "/" + contact.Name}); err != nil {
		return failed(ctx, err, "Failed to list the contact's memberships")
	}
	for i := range memberships.Items {
		if response := rc.leaveIfMarketing(ctx, contact, &memberships.Items[i]); response.HttpStatus != http.StatusOK {
			return response
		}
	}
	return OkResponse()
}

func (rc *receiver) leaveIfMarketing(
	ctx context.Context,
	contact *notificationmiloapiscomv1alpha1.Contact,
	membership *notificationmiloapiscomv1alpha1.ContactGroupMembership,
) Response {
	if !membership.DeletionTimestamp.IsZero() {
		return OkResponse()
	}
	ref := membership.Spec.ContactGroupRef
	group := &notificationmiloapiscomv1alpha1.ContactGroup{}
	if err := rc.client.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, group); err != nil {
		if apierrors.IsNotFound(err) {
			return OkResponse()
		}
		return failed(ctx, err, "Failed to get the membership's contact group")
	}
	if !carriesMarketing(group) {
		return OkResponse()
	}
	return rc.removeFromGroup(ctx, contact, group)
}

// carriesMarketing reports whether a group's Loops list carries marketing: a public group that a Loops list serves.
// A private group's membership is enforced, so a global unsubscribe leaves it in place.
func carriesMarketing(group *notificationmiloapiscomv1alpha1.ContactGroup) bool {
	return group.Spec.Visibility == notificationmiloapiscomv1alpha1.ContactGroupVisibilityPublic &&
		len(groupLoopsListID(group)) > 0
}
