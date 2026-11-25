package loops

import "context"

// API defines the interface for the Loops SDK.
type API interface {
	// UpsertContact creates or updates a contact in Loops.
	UpsertContact(ctx context.Context, req ContactRequest) (*APIResponse, error)

	// DeleteContact deletes a contact from Loops.
	DeleteContact(ctx context.Context, userID string) (*APIResponse, error)

	// AddToMailingList adds a contact to a specific mailing list.
	AddToMailingList(ctx context.Context, userID string, listID string) (*APIResponse, error)

	// RemoveFromMailingList removes a contact from a specific mailing list.
	RemoveFromMailingList(ctx context.Context, userID string, listID string) (*APIResponse, error)
}

// MailingListLister lists the account's mailing lists in Loops.
type MailingListLister interface {
	// ListMailingLists lists the account's mailing lists.
	ListMailingLists(ctx context.Context) ([]MailingList, error)
}
