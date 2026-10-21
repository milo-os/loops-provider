package loops

// WebhookEvent represents the base structure for all Loops webhook events.
type WebhookEvent struct {
	EventName            string          `json:"eventName"`
	EventTime            int64           `json:"eventTime"`
	WebhookSchemaVersion string          `json:"webhookSchemaVersion"`
	ContactIdentity      ContactIdentity `json:"contactIdentity"`
}

// ContactIdentity represents the contact information in webhook events.
type ContactIdentity struct {
	ID     string `json:"id"`
	Email  string `json:"email"`
	UserID string `json:"userId"`
}

// MailingList represents the mailing list information in webhook events.
type MailingList struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	IsPublic    bool   `json:"isPublic"`
}

// MailingListSubscribedEvent represents the contact.mailingList.subscribed webhook event.
type MailingListSubscribedEvent struct {
	WebhookEvent
	MailingList MailingList `json:"mailingList"`
}

// MailingListUnsubscribedEvent represents the contact.mailingList.unsubscribed webhook event.
type MailingListUnsubscribedEvent struct {
	WebhookEvent
	MailingList MailingList `json:"mailingList"`
}

// EventName constants for webhook events.
const (
	EventNameMailingListSubscribed   = "contact.mailingList.subscribed"
	EventNameMailingListUnsubscribed = "contact.mailingList.unsubscribed"
	// EventNameContactDeleted is sent when a contact is deleted from the audience.
	EventNameContactDeleted = "contact.deleted"
	// EventNameContactUnsubscribed is sent when a contact leaves the audience, after a hard bounce, and beside
	// contact.deleted.
	EventNameContactUnsubscribed = "contact.unsubscribed"
	// EventNameEmailResubscribed is sent when a recipient resubscribes from an email's preference center.
	EventNameEmailResubscribed = "email.resubscribed"
)
