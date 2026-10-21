package util

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DuplicateRefusal reports whether err is Milo's admission refusing a create because an object for the same contact
// and group already exists under another name, and returns the refusal's message. Milo answers a second membership or
// a second Removal for one contact and group with an Invalid error whose cause is FieldValueDuplicate, which
// apierrors.IsAlreadyExists does not see.
func DuplicateRefusal(err error) (string, bool) {
	if !apierrors.IsInvalid(err) {
		return "", false
	}
	statusErr, ok := err.(*apierrors.StatusError)
	if !ok || statusErr.ErrStatus.Details == nil {
		return "", false
	}
	for _, cause := range statusErr.ErrStatus.Details.Causes {
		if cause.Type == metav1.CauseTypeFieldValueDuplicate {
			return cause.Message, true
		}
	}
	return "", false
}
