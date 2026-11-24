package controller

import (
	"context"
	"fmt"

	"go.miloapis.com/email-provider-loops/internal/util"
	loops "go.miloapis.com/email-provider-loops/pkg/loops"
	notificationmiloapiscomv1alpha1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// LoopsContactGroupReadyCondition reports, on a contact group that lists Loops, whether Loops has the group's
	// mailing list
	LoopsContactGroupReadyCondition = "LoopsContactGroupReady"
	// LoopsMailingListFoundReason is set when Loops has the group's mailing list
	LoopsMailingListFoundReason = "MailingListFound"
	// LoopsMailingListUnknownReason is set when Loops has no mailing list with the group's ID
	LoopsMailingListUnknownReason = "MailingListUnknown"
	// LoopsMailingListIDMissingReason is set when the group lists Loops without a mailing list ID
	LoopsMailingListIDMissingReason = "MailingListIDMissing"
)

// LoopsContactGroupController reports on each contact group that lists Loops whether Loops has its mailing list, so a
// group that names a mailing list Loops does not have is seen before anyone joins or leaves it.
type LoopsContactGroupController struct {
	Client client.Client
	Loops  loops.MailingListLister
}

// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contactgroups,verbs=get;list;watch
// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contactgroups/status,verbs=get;update;patch

// Reconcile sets the contact group's Loops condition.
func (r *LoopsContactGroupController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cg := &notificationmiloapiscomv1alpha1.ContactGroup{}
	if err := r.Client.Get(ctx, req.NamespacedName, cg); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get contactgroup: %w", err)
	}

	provider, listed := loopsProvider(cg)
	if !listed {
		return ctrl.Result{}, r.forget(ctx, cg)
	}
	if provider.ID == "" {
		return ctrl.Result{}, r.report(ctx, cg, metav1.Condition{
			Type:    LoopsContactGroupReadyCondition,
			Status:  metav1.ConditionFalse,
			Reason:  LoopsMailingListIDMissingReason,
			Message: "The contact group lists Loops but has no mailing list ID",
		})
	}
	lists, err := r.Loops.ListMailingLists(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list Loops mailing lists: %w", err)
	}
	return ctrl.Result{}, r.report(ctx, cg, mailingListCondition(provider.ID, lists))
}

// mailingListCondition is the group's Loops condition for the mailing list ID id, given the account's lists.
func mailingListCondition(id string, lists []loops.MailingList) metav1.Condition {
	for _, list := range lists {
		if list.ID == id {
			return metav1.Condition{
				Type:    LoopsContactGroupReadyCondition,
				Status:  metav1.ConditionTrue,
				Reason:  LoopsMailingListFoundReason,
				Message: fmt.Sprintf("Loops has mailing list %q", id),
			}
		}
	}
	return metav1.Condition{
		Type:    LoopsContactGroupReadyCondition,
		Status:  metav1.ConditionFalse,
		Reason:  LoopsMailingListUnknownReason,
		Message: fmt.Sprintf("Loops has no mailing list with ID %q", id),
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *LoopsContactGroupController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&notificationmiloapiscomv1alpha1.ContactGroup{}).
		Named("loopscontactgroup").
		Complete(r)
}

// report sets cond on the contact group's status, and writes it only when it changed.
func (r *LoopsContactGroupController) report(ctx context.Context, cg *notificationmiloapiscomv1alpha1.ContactGroup, cond metav1.Condition) error {
	cond.ObservedGeneration = cg.GetGeneration()
	err := util.PatchStatusWithLock(ctx, r.Client, cg, func() bool {
		return meta.SetStatusCondition(&cg.Status.Conditions, cond)
	})
	if err != nil {
		return fmt.Errorf("failed to patch contactgroup status: %w", err)
	}
	return nil
}

// forget removes the Loops condition from a contact group that no longer lists Loops, and writes only when the group
// had one.
func (r *LoopsContactGroupController) forget(ctx context.Context, cg *notificationmiloapiscomv1alpha1.ContactGroup) error {
	err := util.PatchStatusWithLock(ctx, r.Client, cg, func() bool {
		return meta.RemoveStatusCondition(&cg.Status.Conditions, LoopsContactGroupReadyCondition)
	})
	if err != nil {
		return fmt.Errorf("failed to remove the Loops condition from the contactgroup status: %w", err)
	}
	return nil
}
