package loops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	defaultBaseURL = "https://app.loops.so/api/v1"
)

// Client is the Loops API client.
type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

// ClientOption defines a functional option for configuring the Client.
type ClientOption func(*Client)

// WithBaseURL sets a custom base URL for the client.
func WithBaseURL(url string) ClientOption {
	return func(c *Client) {
		c.baseURL = url
	}
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = client
	}
}

// NewSDK creates a new Loops API client.
func NewSDK(apiKey string, opts ...ClientOption) (*Client, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("api key is required")
	}

	c := &Client{
		apiKey:     apiKey,
		baseURL:    defaultBaseURL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.baseURL == "" {
		return nil, fmt.Errorf("base url is required")
	}

	return c, nil
}

// ContactRequest represents the payload for creating or updating a contact.
type ContactRequest struct {
	Email        string          `json:"email,omitempty"`
	UserID       string          `json:"userId,omitempty"`
	FirstName    string          `json:"firstName,omitempty"`
	LastName     string          `json:"lastName,omitempty"`
	Source       string          `json:"source,omitempty"`
	Subscribed   *bool           `json:"subscribed,omitempty"`
	UserGroup    string          `json:"userGroup,omitempty"`
	MailingLists map[string]bool `json:"mailingLists,omitempty"`
}

// APIResponse represents a generic response from the Loops API.
//
// On Success: Returns Success=true and an ID.
// Note: The returned ID is an operation ID from the provider and should NOT be used for tracking purposes or as a
// persistent reference.
//
// On Failure: Returns Success=false and a Message describing the error.
type APIResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	ID      string `json:"id,omitempty"`
}

func (c *Client) sendRequest(ctx context.Context, method, path string, body interface{}, out interface{}) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("%s%s", c.baseURL, path), bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return &Error{
			StatusCode: resp.StatusCode,
			Body:       string(respBody),
		}
	}

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}

// UpsertContact creates or updates a contact in Loops.
//
// API: PUT /contacts/update
//
// Idempotency: Idempotent
//
// Errors:
//   - 400 Bad Request: If the request payload is invalid.
func (c *Client) UpsertContact(ctx context.Context, req ContactRequest) (*APIResponse, error) {
	var resp APIResponse
	err := c.sendRequest(ctx, http.MethodPut, "/contacts/update", req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeleteContactRequest represents the payload for deleting a contact.
type DeleteContactRequest struct {
	UserID string `json:"userId"`
}

// DeleteContact deletes a contact from Loops.
//
// API: POST /contacts/delete
//
// Idempotency: Not idempotent
//
// Errors:
//   - 404 Not Found: If the contact does not exist.
//   - 400 Bad Request: If the request is invalid.
func (c *Client) DeleteContact(ctx context.Context, userID string) (*APIResponse, error) {
	req := DeleteContactRequest{UserID: userID}
	var resp APIResponse
	err := c.sendRequest(ctx, http.MethodPost, "/contacts/delete", req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// AddToMailingList adds a contact to a specific mailing list.
//
// Convenience wrapper around UpsertContact.
//
// Idempotency: Idempotent
//
// Errors:
//   - 400 Bad Request: If the request payload is invalid.
func (c *Client) AddToMailingList(ctx context.Context, userID string, listID string) (*APIResponse, error) {
	req := ContactRequest{
		UserID: userID,
		MailingLists: map[string]bool{
			listID: true,
		},
	}
	return c.UpsertContact(ctx, req)
}

// ListMailingLists lists the account's mailing lists.
//
// API: GET /lists
//
// Idempotency: Idempotent
func (c *Client) ListMailingLists(ctx context.Context) ([]MailingList, error) {
	var lists []MailingList
	if err := c.sendRequest(ctx, http.MethodGet, "/lists", nil, &lists); err != nil {
		return nil, err
	}
	return lists, nil
}

// RemoveFromMailingList removes a contact from a specific mailing list.
//
// Convenience wrapper around UpsertContact.
//
// Idempotency: Idempotent
//
// Errors:
//   - 400 Bad Request: If the request payload is invalid.
func (c *Client) RemoveFromMailingList(ctx context.Context, userID string, listID string) (*APIResponse, error) {
	req := ContactRequest{
		UserID: userID,
		MailingLists: map[string]bool{
			listID: false,
		},
	}
	return c.UpsertContact(ctx, req)
}
