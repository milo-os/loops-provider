package controller

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

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
	loopsContactFinalizerKey = "notification.miloapis.com/loops-contact"
)

const (
	// LoopsContactReadyCondition is a condition that is set to true when the Loops contact is ready
	LoopsContactReadyCondition = "LoopsContactReady"
	// ContactNotCreatedReason is a reason that is set when the Loops contact is not created
	LoopsContactNotCreatedReason = "ContactNotCreated"
	// ContactCreatedReason is a reason that is set when the Loops contact is created
	LoopsContactCreatedReason = "ContactCreated"
	// ContactUpdatedReason is a reason that is set when the Loops contact is updated
	LoopsContactUpdatedReason = "ContactUpdated"
	// ContactNotUpdatedReason is a reason that is set when the Loops contact is not updated
	LoopsContactNotUpdatedReason = "ContactNotUpdated"
	// LoopsContactNotFinalizedReason is a reason that is set when the Loops contact is not finalized
	LoopsContactNotFinalizedReason = "ContactNotFinalized"
	// LoopsContactMissingInLoopsReason is a reason that is set when Loops reports the contact deleted
	LoopsContactMissingInLoopsReason = "ContactMissingInLoops"
)

const (
	// LoopsSubscribedCondition is Loops' audience state of the contact, as Loops' last event reported it
	LoopsSubscribedCondition = "LoopsSubscribed"
	// LoopsUnsubscribedReason is a reason that is set when Loops reports the contact unsubscribed from the audience
	LoopsUnsubscribedReason = "UnsubscribedInLoops"
	// LoopsResubscribedReason is a reason that is set when Loops reports the contact resubscribed
	LoopsResubscribedReason = "ResubscribedInLoops"
)

const (
	// NewsLetterAddedCondition is a condition that is set to true when the mailing list is added to the Loops contact
	NewsLetterAddedCondition = "NewsLetterAdded"
	// NewsLetterAddedReason is a reason that is set when the mailing list is added to the Loops contact
	NewsLetterAddedReason = "NewsLetterAdded"
	// NewsLetterNotAddedReason is a reason that is set when the mailing list is not added to the Loops contact
	NewsLetterNotAddedReason = "NewsLetterNotAdded"
)

// LoopsContactReconciler reconciles a LoopsContact object
type LoopsContactController struct {
	Client                          client.Client
	Finalizers                      finalizer.Finalizers
	Loops                           loops.API
	NewsLetterContactGroupName      string
	NewsLetterContactGroupNamespace string
}

// loopsContactFinalizer is a finalizer for the Contact object
type loopsContactFinalizer struct {
	Client client.Client
	Loops  loops.API
}

func (f *loopsContactFinalizer) Finalize(ctx context.Context, obj client.Object) (finalizer.Result, error) {
	log := logf.FromContext(ctx).WithValues("finalizer", "ContactFinalizer", "trigger", obj.GetName())
	log.Info("Finalizing Contact")

	// Type assertion
	contact, ok := obj.(*notificationmiloapiscomv1alpha1.Contact)
	if !ok {
		log.Error(fmt.Errorf("object is not a Contact"), "Failed to finalize Contact")
		return finalizer.Result{}, fmt.Errorf("object is not a Contact")
	}

	var finalizerError error

	// Delete Loops contact
	err := f.DeleteContact(ctx, contact)
	if err != nil {
		log.Error(err, "Failed to delete Loops contact")
		finalizerError = err
	}

	if finalizerError != nil {
		original := contact.DeepCopy()
		oldStatus := contact.Status.DeepCopy()

		meta.SetStatusCondition(&contact.Status.Conditions, metav1.Condition{
			Type:               LoopsContactReadyCondition,
			Status:             metav1.ConditionFalse,
			Reason:             LoopsContactNotFinalizedReason,
			Message:            fmt.Sprintf("Failed to remove Loops contact: %s", finalizerError.Error()),
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: contact.GetGeneration(),
		})

		if err := util.PatchStatusIfChanged(ctx, util.StatusPatchParams{
			Client:     f.Client,
			Logger:     log,
			Object:     contact,
			Original:   original,
			OldStatus:  oldStatus,
			NewStatus:  &contact.Status,
			FieldOwner: "loopscontact-controller",
		}); err != nil {
			log.Error(err, "Failed to patch contact status in finalizer")
			finalizerError = err
		}

		return finalizer.Result{}, finalizerError
	}

	return finalizer.Result{}, nil
}

// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contacts,verbs=get;list;watch
// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contacts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contacts/finalizers,verbs=update
// +kubebuilder:rbac:groups=notification.miloapis.com,resources=contactgroupmemberships,verbs=get;list;watch;delete

// Reconcile is the main function that reconciles the Contact object.
func (r *LoopsContactController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("controller", "ContactController", "trigger", req.NamespacedName)
	log.Info("Starting reconciliation", "namespacedName", req.String(), "name", req.Name, "namespace", req.Namespace)

	// Get Contact
	contact := &notificationmiloapiscomv1alpha1.Contact{}
	err := r.Client.Get(ctx, req.NamespacedName, contact)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("Contact not found. Probably deleted.")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get contact: %w", err)
	}

	// Run finalizers
	finalizeResult, err := r.Finalizers.Finalize(ctx, contact)
	if err != nil {
		log.Error(err, "Failed to run finalizers for Contact")
		return ctrl.Result{}, fmt.Errorf("failed to run finalizers for Contact: %w", err)
	}
	if finalizeResult.Updated {
		log.Info("finalizer updated the contact object, updating API server")
		if updateErr := r.Client.Update(ctx, contact); updateErr != nil {
			if errors.IsConflict(updateErr) {
				log.Info("Conflict updating Contact after finalizer update; requeuing")
				return ctrl.Result{Requeue: true}, nil
			}
			log.Error(updateErr, "Failed to update Contact after finalizer update")
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, nil
	}

	var reconcileError error
	oldStatus := contact.Status.DeepCopy()
	original := contact.DeepCopy()
	readyCond := meta.FindStatusCondition(contact.Status.Conditions, LoopsContactReadyCondition)

	switch {
	// First creation – condition not present yet
	case readyCond == nil || readyCond.Reason == LoopsContactNotCreatedReason:
		log.Info("LoopsContact creation")

		err := r.upsertContact(ctx, contact)
		if err != nil {
			reconcileError = err
			log.Info("Bad Request when creating Loops contact")
			meta.SetStatusCondition(&contact.Status.Conditions, metav1.Condition{
				Type:               LoopsContactReadyCondition,
				Status:             metav1.ConditionFalse,
				Reason:             LoopsContactNotCreatedReason,
				Message:            fmt.Sprintf("Loops contact not created on email provider: %s", err.Error()),
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: contact.GetGeneration(),
			})
		}

		if err == nil {
			log.Info("Loops contact created")
			meta.SetStatusCondition(&contact.Status.Conditions, metav1.Condition{
				Type:               LoopsContactReadyCondition,
				Status:             metav1.ConditionTrue,
				Reason:             LoopsContactCreatedReason,
				Message:            "Loops contact created on email provider",
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: contact.GetGeneration(),
			})
			contact.Status.Providers = []notificationmiloapiscomv1alpha1.ContactProviderStatus{
				{
					Name: "Loops",
					ID:   string(contact.UID),
				},
			}
		}

	// Update – generation changed since we last processed the object
	case readyCond.ObservedGeneration != contact.GetGeneration() || readyCond.Reason == LoopsContactNotUpdatedReason:
		log.Info("Contact updated")

		err := r.upsertContact(ctx, contact)
		if err != nil {
			reconcileError = err
			log.Error(err, "Failed to update contact on email provider")
			meta.SetStatusCondition(&contact.Status.Conditions, metav1.Condition{
				Type:               LoopsContactReadyCondition,
				Status:             metav1.ConditionFalse,
				Reason:             LoopsContactNotUpdatedReason,
				Message:            fmt.Sprintf("Loops contact not updated on email provider: %s", err.Error()),
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: contact.GetGeneration(),
			})
		}

		if err == nil {
			log.Info("Loops contact updated")
			meta.SetStatusCondition(&contact.Status.Conditions, metav1.Condition{
				Type:               LoopsContactReadyCondition,
				Status:             metav1.ConditionTrue,
				Reason:             LoopsContactUpdatedReason,
				Message:            "Loops contact updated on email provider",
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: contact.GetGeneration(),
			})
		}
	}

	errorAddingToNewsLetter := false
	if r.isNewsletterContact(contact) {
		errorAddingToNewsLetter = r.addToNewsLetterList(ctx, contact)
	}

	// Update contact status if it changed
	if err := util.PatchStatusIfChanged(ctx, util.StatusPatchParams{
		Client:     r.Client,
		Logger:     log,
		Object:     contact,
		Original:   original,
		OldStatus:  oldStatus,
		NewStatus:  &contact.Status,
		FieldOwner: "loopscontact-controller",
	}); err != nil {
		return ctrl.Result{}, err
	}

	if reconcileError != nil {
		return ctrl.Result{}, reconcileError
	}

	if errorAddingToNewsLetter {
		log.Error(errors.NewInternalError(fmt.Errorf("failed to add mailing list to Loops contact")), "Failed to add mailing list to Loops contact")
		return ctrl.Result{}, fmt.Errorf("failed to add mailing list to Loops contact")
	}

	log.Info("Contact reconciled")

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *LoopsContactController) SetupWithManager(mgr ctrl.Manager) error {
	// Register finalizer
	r.Finalizers = finalizer.NewFinalizers()
	if err := r.Finalizers.Register(loopsContactFinalizerKey, &loopsContactFinalizer{
		Client: r.Client,
		Loops:  r.Loops,
	}); err != nil {
		return fmt.Errorf("failed to register loops contact finalizer: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&notificationmiloapiscomv1alpha1.Contact{}).
		Named("loopscontact").
		Complete(r)
}

func (r *LoopsContactController) upsertContact(ctx context.Context, contact *notificationmiloapiscomv1alpha1.Contact) error {
	log := logf.FromContext(ctx).WithValues("controller", "LoopsContactController", "trigger", contact.Name)
	log.Info("Creating Loops contact")

	// Upsert the Loops contact. Subscribed is left out: Loops subscribes a new contact by default, and on an existing
	// contact the field re-subscribes a person who unsubscribed in Loops.
	_, err := r.Loops.UpsertContact(ctx, loops.ContactRequest{
		Email:     contact.Spec.Email,
		UserID:    string(contact.UID),
		FirstName: contact.Spec.GivenName,
		LastName:  contact.Spec.FamilyName,
		Source:    "email-provider-loops-k8s-controller",
	})
	if err != nil {
		log.Error(err, "Failed to find Loops contact")
		return fmt.Errorf("failed to find Loops contact: %w", err)
	}

	return nil
}

func (f *loopsContactFinalizer) DeleteContact(ctx context.Context, contact *notificationmiloapiscomv1alpha1.Contact) error {
	log := logf.FromContext(ctx).WithValues("controller", "LoopsContactController", "trigger", contact.Name)
	log.Info("Deleting Loops contact")

	// Delete Loops contact
	_, err := f.Loops.DeleteContact(ctx, string(contact.UID))
	if err != nil {
		if !loops.IsNotFound(err) {
			log.Error(err, "Failed to delete Loops contact")
			return fmt.Errorf("failed to delete Loops contact: %w", err)
		}
		log.Info("Loops contact not found, probably deleted already")
	}

	return nil
}

// isNewsletterContact returns true if the contact name starts with "newsletter-".
func (r *LoopsContactController) isNewsletterContact(contact *notificationmiloapiscomv1alpha1.Contact) bool {
	return strings.HasPrefix(contact.Name, "newsletter-")
}

func (r *LoopsContactController) addToNewsLetterList(ctx context.Context, contact *notificationmiloapiscomv1alpha1.Contact) bool {
	log := logf.FromContext(ctx).WithValues("controller", "LoopsContactController", "trigger", contact.Name)
	log.Info("Adding mailing list to Loops contact")

	newsLetterCond := meta.FindStatusCondition(contact.Status.Conditions, NewsLetterAddedCondition)
	if newsLetterCond != nil && newsLetterCond.Status == metav1.ConditionTrue {
		log.Info("News letter already added")
		return false
	}

	// Add mailing list to Loops contact
	contactgroupmembership := notificationmiloapiscomv1alpha1.ContactGroupMembership{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.generateCgmName(contact),
			Namespace: contact.Namespace,
		},
		Spec: notificationmiloapiscomv1alpha1.ContactGroupMembershipSpec{
			ContactRef: notificationmiloapiscomv1alpha1.ContactReference{
				Name:      contact.Name,
				Namespace: contact.Namespace,
			},
			ContactGroupRef: notificationmiloapiscomv1alpha1.ContactGroupReference{
				Name:      r.NewsLetterContactGroupName,
				Namespace: r.NewsLetterContactGroupNamespace,
			},
		},
	}

	if err := r.Client.Create(ctx, &contactgroupmembership); err != nil {
		if errors.IsAlreadyExists(err) {
			log.Info("ContactGroupMembership already exists")
			return false
		}
		if reason, alreadyMember := util.DuplicateRefusal(err); alreadyMember {
			// A user could already have access to the newsletter because of a
			// previously created membership under a different name. This
			// prevents the reconciler from endlessly retrying a create that
			// Milo's webhook will always reject as a duplicate.
			log.Info("Contact already a member of the newsletter group", "reason", reason)
			meta.SetStatusCondition(&contact.Status.Conditions, metav1.Condition{
				Type:               NewsLetterAddedCondition,
				Status:             metav1.ConditionTrue,
				Reason:             NewsLetterAddedReason,
				Message:            "Contact already added to Newsletter list on email provider.",
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: contact.GetGeneration(),
			})
			return false
		}
		log.Error(err, "Failed to create ContactGroupMembership")

		meta.SetStatusCondition(&contact.Status.Conditions, metav1.Condition{
			Type:               NewsLetterAddedCondition,
			Status:             metav1.ConditionFalse,
			Reason:             NewsLetterNotAddedReason,
			Message:            fmt.Sprintf("Contact not added to Newsletter list: %s", err.Error()),
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: contact.GetGeneration(),
		})

		return true
	}

	meta.SetStatusCondition(&contact.Status.Conditions, metav1.Condition{
		Type:               NewsLetterAddedCondition,
		Status:             metav1.ConditionTrue,
		Reason:             NewsLetterAddedReason,
		Message:            "Contact added to Newsletter list on email provider.",
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: contact.GetGeneration(),
	})

	log.Info("ContactGroupMembership created")
	return false
}

// generateCgmName generates a deterministic name for a ContactGroupMembership
func (r *LoopsContactController) generateCgmName(
	contact *notificationmiloapiscomv1alpha1.Contact,
) string {
	// Create a full hash for uniqueness
	hash := sha256.Sum256([]byte(string(contact.UID)))
	hashStr := fmt.Sprintf("%x", hash)

	return fmt.Sprintf("%s-%s", contact.Name, hashStr)
}
