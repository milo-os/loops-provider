package util

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StatusPatchParams holds the parameters for patching status.
type StatusPatchParams struct {
	Client     client.Client
	Logger     logr.Logger
	Object     client.Object
	Original   client.Object
	OldStatus  any
	NewStatus  any
	FieldOwner string
}

// PatchStatusIfChanged checks if the status has changed and patches it if so.
func PatchStatusIfChanged(ctx context.Context, params StatusPatchParams) error {
	if !equality.Semantic.DeepEqual(params.OldStatus, params.NewStatus) {
		if err := params.Client.Status().Patch(ctx, params.Object, client.MergeFrom(params.Original), client.FieldOwner(params.FieldOwner)); err != nil {
			params.Logger.Error(err, "Failed to patch resource status")
			return fmt.Errorf("failed to patch resource status: %w", err)
		}
	} else {
		params.Logger.Info("Resource status unchanged, skipping update")
	}

	return nil
}

// PatchStatusWithLock reads obj again, lets mutate change it, and patches its status under the optimistic lock, so a
// condition another writer set after our read is never dropped. A conflict starts again from a fresh read. mutate
// reports whether it changed anything; nothing is written when it did not.
//
// The backoff is client-go's DefaultBackoff, recommended "for a conflict where a client may be attempting to make an
// unrelated modification to a resource under active management by one or more controllers"; it also gives a cached
// read time to catch up with the write that conflicted.
func PatchStatusWithLock(ctx context.Context, c client.Client, obj client.Object, mutate func() bool) error {
	key := client.ObjectKeyFromObject(obj)
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if err := c.Get(ctx, key, obj); err != nil {
			return err
		}
		original := obj.DeepCopyObject().(client.Object)
		if !mutate() {
			return nil
		}
		return c.Status().Patch(ctx, obj, client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{}))
	})
}
