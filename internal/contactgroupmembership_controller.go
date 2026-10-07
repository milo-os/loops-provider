package controller

import (
	"context"
	stderrors "errors"
	"fmt"

	"go.miloapis.com/email-provider-loops/internal/util"
	loops "go.miloapis.com/email-provider-loops/pkg/loops"
	notificationmiloapiscomv1alpha1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/finalizer"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// LoopsContactGroupMembershipReadyCondition is a condition that is set to true when the Loops contact group membership is ready
	LoopsContactGroupMembershipReadyCondition = "LoopsContactGroupMembershipReady"
	// ContactGroupMembershipNotCreatedReason is a reason that is set when the Loops contact group membership is not created
	LoopsContactGroupMembershipNotCreatedReason = "ContactGroupMembershipNotCreated"
	// ContactGroupMembershipCreatedReason is a reason that is set when the Loops contact group membership is created
	LoopsContactGroupMembershipCreatedReason = "ContactGroupMembershipCreated"
	// LoopsContactGroupMembershipNotFinalizedReason is a reason that is set when the Loops contact group membership is not finalized
	LoopsContactGroupMembershipNotFinalizedReason = "ContactGroupMembershipNotFinalized"
)

const (
	loopsContactGroupMembershipFinalizerKey = "notification.miloapis.com/loops-contact-group-membership"
)

// errMailingListIDNotFound is returned by getMailingListId when the referenced
// ContactGroup has no Loops provider configured. It is treated as a terminal
// condition by the finalizer: without a mailing list ID, no Loops-side
// membership can exist for this ContactGroupMembership.
var errMailingListIDNotFound = stderrors.New("mailing list ID not found for contact group")

// LoopsContactGroupMembershipReconciler reconciles a LoopsContact object
type LoopsContactGroupMembershipController struct {
	Client     client.Client
	Finalizers finalizer.Finalizers
	Loops      loops.API
}

// loopsContactGroupMembershipController is a finalizer for the Contact object
type loopsContactGroupMembershipFinalizer struct {
	Client client.Client
	Loops  loops.API
}

func (f *loopsContactGroupMembershipFinalizer) Finalize(ctx context.Context, obj client.Object) (finalizer.Result, error) {
	log := logf.FromContext(ctx).WithValues("finalizer", "ContactGroupMembershipFinalizer", "trigger", obj.GetName())
	log.Info("Finalizing ContactGroupMembership")

	// Type assertion
	cgm, ok := obj.(*notificationmiloapiscomv1alpha1.ContactGroupMembership)
	if !ok {
		log.Error(fmt.Errorf("object is not a ContactGroupMembership"), "Failed to finalize ContactGroupMembership")
		return finalizer.Result{}, fmt.Errorf("object is not a ContactGroupMembership")
	}

	var finalizerError error

	// Get referenced resources
	contact, contactGroup, err := getReferencedResources(ctx, f.Client, cgm)
	if err != nil {
		// If the referenced Contact or ContactGroup no longer exists, the
		// Loops-side membership either was already cleaned up (by the Contact
		// finalizer, which removes the Loops contact from all mailing lists) or
		// no longer matters. Hard-failing here would deadlock the finalizer
		// (and the parent ContactGroup deletion), so complete instead.
		if errors.IsNotFound(err) {
			log.Info("Referenced Contact or ContactGroup not found. Probably deleted. ContactGroupMembership finalizer completed.")
			return finalizer.Result{}, nil
		}
		log.Error(err, "Failed to get referenced resources")
		finalizerError = fmt.Errorf("failed to get referenced resources: %w", err)
	}

	// Delete Loops contact
	if finalizerError == nil {
		err = f.removeContactFromMailingList(ctx, contact, contactGroup)
		if err != nil {
			if errors.IsNotFound(err) || loops.IsNotFound(err) || stderrors.Is(err, errMailingListIDNotFound) {
				// Nothing exists on the Loops side for this membership anymore
				// (or the group was never configured with a Loops mailing
				// list), so the finalizer can complete.
				log.Info("Loops membership not found, probably deleted already. ContactGroupMembership finalizer completed.")
				return finalizer.Result{}, nil
			}
			log.Error(err, "Failed to delete Loops contact")
			finalizerError = fmt.Errorf("failed to delete Loops contact: %w", err)
		}
	}

	// Create a copy for the patch base
	original := cgm.DeepCopy()

	if finalizerError != nil {
		oldStatus := cgm.Status.DeepCopy()

		meta.SetStatusCondition(&cgm.Status.Conditions, metav1.Condition{
			Type:               LoopsContactGroupMembershipReadyCondition,
			Status:             metav1.ConditionFalse,
			Reason:             LoopsContactGroupMembershipNotFinalizedReason,
			Message:            fmt.Sprintf("Failed to remove Loops contact from mailing list: %s", err.Error()),
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: cgm.GetGeneration(),
		})

		err = util.PatchStatusIfChanged(ctx, util.StatusPatchParams{
			Client:     f.Client,
			Logger:     log,
			Object:     cgm,
			Original:   original,
			OldStatus:  oldStatus,
			NewStatus:  &cgm.Status,
			FieldOwner: "loopscontactgroupmembership-controller",
		})
		if err != nil {
			log.Error(err, "Failed to patch contactgroupmembership status in finalizer")
			finalizerError = fmt.Errorf("failed to patch contactgroupmembership status in finalizer: %w", err)
		}

		return finalizer.Result{}, finalizerError
	}

	return finalizer.Result{}, nil
}

// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contactgroupmemberships,verbs=get;list;watch
// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contactgroupmemberships/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contactgroupmemberships/finalizers,verbs=update

// Reconcile is the main function that reconciles the ContactGroupMembership object.
func (r *LoopsContactGroupMembershipController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("controller", "ContactGroupMembershipController", "trigger", req.NamespacedName)
	log.Info("Starting reconciliation", "namespacedName", req.String(), "name", req.Name, "namespace", req.Namespace)

	// Get ContactGroupMembership
	cgm := &notificationmiloapiscomv1alpha1.ContactGroupMembership{}
	err := r.Client.Get(ctx, req.NamespacedName, cgm)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("ContactGroupMembership not found. Probably deleted.")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get contactgroupmembership: %w", err)
	}

	// Run finalizers
	finalizeResult, err := r.Finalizers.Finalize(ctx, cgm)
	if err != nil {
		log.Error(err, "Failed to run finalizers for ContactGroupMembership")
		return ctrl.Result{}, fmt.Errorf("failed to run finalizers for ContactGroupMembership: %w", err)
	}
	if finalizeResult.Updated {
		log.Info("finalizer updated the contactgroupmembership object, updating API server")
		if updateErr := r.Client.Update(ctx, cgm); updateErr != nil {
			if errors.IsConflict(updateErr) {
				log.Info("Conflict updating ContactGroupMembership after finalizer update; requeuing")
				return ctrl.Result{Requeue: true}, nil
			}
			log.Error(updateErr, "Failed to update ContactGroupMembership after finalizer update")
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, nil
	}

	var reconcileError error

	// Get Referenced resources
	contact, contactGroup, err := getReferencedResources(ctx, r.Client, cgm)
	if err != nil {
		log.Error(err, "Failed to get referenced resources")
		reconcileError = fmt.Errorf("failed to get referenced resources: %w", err)
		meta.SetStatusCondition(&cgm.Status.Conditions, metav1.Condition{
			Type:               LoopsContactGroupMembershipReadyCondition,
			Status:             metav1.ConditionFalse,
			Reason:             LoopsContactGroupMembershipNotCreatedReason,
			Message:            fmt.Sprintf("Failed to get referenced resources: %s", err.Error()),
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: cgm.GetGeneration(),
		})

	}

	oldStatus := cgm.Status.DeepCopy()
	original := cgm.DeepCopy()
	readyCond := meta.FindStatusCondition(cgm.Status.Conditions, LoopsContactGroupMembershipReadyCondition)

	if (readyCond == nil || readyCond.Reason == LoopsContactGroupMembershipNotCreatedReason) && reconcileError == nil {
		log.Info("LoopsContact creation")

		err = r.addContactToMailingList(ctx, contact, contactGroup)
		if err != nil {
			reconcileError = err
			log.Error(err, "Failed to add contact to mailing list")
			meta.SetStatusCondition(&cgm.Status.Conditions, metav1.Condition{
				Type:               LoopsContactGroupMembershipReadyCondition,
				Status:             metav1.ConditionFalse,
				Reason:             LoopsContactGroupMembershipNotCreatedReason,
				Message:            fmt.Sprintf("Loops contact group membership not created on email provider: %s", err.Error()),
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: cgm.GetGeneration(),
			})
		}

		if err == nil {
			log.Info("Loops contact group membership created")
			meta.SetStatusCondition(&cgm.Status.Conditions, metav1.Condition{
				Type:               LoopsContactGroupMembershipReadyCondition,
				Status:             metav1.ConditionTrue,
				Reason:             LoopsContactGroupMembershipCreatedReason,
				Message:            "Loops contact group membership created on email provider",
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: cgm.GetGeneration(),
			})
			cgm.Status.Providers = []notificationmiloapiscomv1alpha1.ContactProviderStatus{
				{
					Name: "Loops",
					ID:   string(contact.UID),
				},
			}
		}
	}

	if err := util.PatchStatusIfChanged(ctx, util.StatusPatchParams{
		Client:     r.Client,
		Logger:     log,
		Object:     cgm,
		Original:   original,
		OldStatus:  oldStatus,
		NewStatus:  &cgm.Status,
		FieldOwner: "loopscontactgroupmembership-controller",
	}); err != nil {
		return ctrl.Result{}, err
	}

	if reconcileError != nil {
		return ctrl.Result{}, reconcileError
	}

	log.Info("Contactgroupmembership reconciled")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *LoopsContactGroupMembershipController) SetupWithManager(mgr ctrl.Manager) error {
	// Register finalizer
	r.Finalizers = finalizer.NewFinalizers()
	if err := r.Finalizers.Register(loopsContactGroupMembershipFinalizerKey, &loopsContactGroupMembershipFinalizer{
		Client: r.Client,
		Loops:  r.Loops,
	}); err != nil {
		return fmt.Errorf("failed to register loops contact group membership finalizer: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&notificationmiloapiscomv1alpha1.ContactGroupMembership{}).
		Named("loopscontactgroupmembership").
		Complete(r)
}

func (r *LoopsContactGroupMembershipController) addContactToMailingList(ctx context.Context, c *notificationmiloapiscomv1alpha1.Contact, cg *notificationmiloapiscomv1alpha1.ContactGroup) error {
	log := logf.FromContext(ctx).WithValues("controller", "LoopsContactGroupMembershipController", "trigger", c.Name)
	log.Info("Adding Loops contact to mailing list")

	mailingListId, err := getMailingListId(cg)
	if err != nil {
		log.Error(err, "Failed to get Loops mailing list ID")
		return fmt.Errorf("failed to get Loops mailing list ID: %w", err)
	}

	_, err = r.Loops.AddToMailingList(ctx, string(c.UID), mailingListId)
	if err != nil {
		log.Error(err, "Failed to add Loops contact to mailing list")
		return fmt.Errorf("failed to add Loops contact to mailing list: %w", err)
	}

	return nil
}

func (f *loopsContactGroupMembershipFinalizer) removeContactFromMailingList(ctx context.Context, c *notificationmiloapiscomv1alpha1.Contact, cg *notificationmiloapiscomv1alpha1.ContactGroup) error {
	log := logf.FromContext(ctx).WithValues("controller", "LoopsContactGroupMembershipController", "trigger", c.Name)
	log.Info("Removing Loops contact from mailing list")

	mailingListId, err := getMailingListId(cg)
	if err != nil {
		log.Error(err, "Failed to get Loops mailing list ID")
		return fmt.Errorf("failed to get Loops mailing list ID: %w", err)
	}

	_, err = f.Loops.RemoveFromMailingList(ctx, string(c.UID), mailingListId)
	if err != nil {
		log.Error(err, "Failed to remove Loops contact from mailing list")
		return fmt.Errorf("failed to remove Loops contact from mailing list: %w", err)
	}

	return nil
}

func getMailingListId(cg *notificationmiloapiscomv1alpha1.ContactGroup) (string, error) {
	for _, provider := range cg.Spec.Providers {
		if provider.Name == "Loops" {
			return provider.ID, nil
		}
	}

	return "", errMailingListIDNotFound
}

func getReferencedResources(ctx context.Context, k8sClient client.Client, cgm *notificationmiloapiscomv1alpha1.ContactGroupMembership) (*notificationmiloapiscomv1alpha1.Contact, *notificationmiloapiscomv1alpha1.ContactGroup, error) {
	// Get Referenced Contact
	contact := &notificationmiloapiscomv1alpha1.Contact{}
	err := k8sClient.Get(ctx, client.ObjectKey{Name: cgm.Spec.ContactRef.Name, Namespace: cgm.Spec.ContactRef.Namespace}, contact)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get Contact: %w", err)
	}

	// Get Referenced ContactGroup
	contactGroup := &notificationmiloapiscomv1alpha1.ContactGroup{}
	err = k8sClient.Get(ctx, client.ObjectKey{Name: cgm.Spec.ContactGroupRef.Name, Namespace: cgm.Spec.ContactGroupRef.Namespace}, contactGroup)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get ContactGroup: %w", err)
	}

	return contact, contactGroup, nil
}
