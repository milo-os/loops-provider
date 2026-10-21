package webhook

import (
	"context"
	"fmt"
	"time"

	controller "go.miloapis.com/email-provider-loops/internal"
	"go.miloapis.com/email-provider-loops/internal/util"
	notificationmiloapiscomv1alpha1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
