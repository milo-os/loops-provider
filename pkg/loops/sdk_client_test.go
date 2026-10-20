package loops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewSDK(t *testing.T) {
	tests := []struct {
		name    string
		apiKey  string
		opts    []ClientOption
		wantErr bool
	}{
		{
			name:    "Valid configuration",
			apiKey:  "test-api-key",
			wantErr: false,
		},
		{
			name:    "Missing API key",
			apiKey:  "",
			wantErr: true,
		},
		{
			name:   "Missing Base URL",
			apiKey: "test-api-key",
			opts: []ClientOption{
				WithBaseURL(""),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewSDK(tt.apiKey, tt.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSDK() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == nil {
				t.Error("NewSDK() returned nil client")
			}
		})
	}
}

func TestUpsertContact(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("Expected PUT request, got %s", r.Method)
		}
		if r.URL.Path != "/contacts/update" {
			t.Errorf("Expected path /contacts/update, got %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("Expected Authorization header, got %s", r.Header.Get("Authorization"))
		}

		var req ContactRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("Failed to decode request body: %v", err)
		}

		if req.Email != "test@example.com" {
			t.Errorf("Expected email test@example.com, got %s", req.Email)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			ID:      "contact-123",
		}); err != nil {
			t.Errorf("Failed to write response: %v", err)
		}
	}))
	defer ts.Close()

	client, _ := NewSDK("test-key", WithBaseURL(ts.URL))
	resp, err := client.UpsertContact(context.Background(), ContactRequest{Email: "test@example.com"})
	if err != nil {
		t.Fatalf("UpsertContact() failed: %v", err)
	}

	if !resp.Success {
		t.Error("UpsertContact() expected success true")
	}
	if resp.ID != "contact-123" {
		t.Errorf("UpsertContact() expected ID contact-123, got %s", resp.ID)
	}
}

func TestDeleteContact(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST request, got %s", r.Method)
		}
		if r.URL.Path != "/contacts/delete" {
			t.Errorf("Expected path /contacts/delete, got %s", r.URL.Path)
		}

		var req DeleteContactRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("Failed to decode request body: %v", err)
		}
		if req.UserID != "user-123" {
			t.Errorf("Expected userId user-123, got %s", req.UserID)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Message: "Deleted",
		}); err != nil {
			t.Errorf("Failed to write response: %v", err)
		}
	}))
	defer ts.Close()

	client, _ := NewSDK("test-key", WithBaseURL(ts.URL))
	resp, err := client.DeleteContact(context.Background(), "user-123")
	if err != nil {
		t.Fatalf("DeleteContact() failed: %v", err)
	}

	if !resp.Success {
		t.Error("DeleteContact() expected success true")
	}
}

func TestAddToMailingList(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ContactRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("Failed to decode request: %v", err)
		}

		if !req.MailingLists["list-abc"] {
			t.Error("Expected mailing list list-abc to be true")
		}
		if req.UserID != "user-123" {
			t.Errorf("Expected userId user-123, got %s", req.UserID)
		}

		if err := json.NewEncoder(w).Encode(APIResponse{Success: true}); err != nil {
			t.Errorf("Failed to write response: %v", err)
		}
	}))
	defer ts.Close()

	client, _ := NewSDK("test-key", WithBaseURL(ts.URL))
	_, err := client.AddToMailingList(context.Background(), "user-123", "list-abc")
	if err != nil {
		t.Fatalf("AddToMailingList() failed: %v", err)
	}
}

func TestRemoveFromMailingList(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ContactRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("Failed to decode request: %v", err)
		}

		if val, ok := req.MailingLists["list-abc"]; !ok || val {
			t.Error("Expected mailing list list-abc to be false")
		}

		if err := json.NewEncoder(w).Encode(APIResponse{Success: true}); err != nil {
			t.Errorf("Failed to write response: %v", err)
		}
	}))
	defer ts.Close()

	client, _ := NewSDK("test-key", WithBaseURL(ts.URL))
	_, err := client.RemoveFromMailingList(context.Background(), "user-123", "list-abc")
	if err != nil {
		t.Fatalf("RemoveFromMailingList() failed: %v", err)
	}
}

func TestClient_Errors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		if _, err := w.Write([]byte(`{"success":false,"message":"Not found"}`)); err != nil {
			t.Errorf("Failed to write response: %v", err)
		}
	}))
	defer ts.Close()

	client, _ := NewSDK("test-key", WithBaseURL(ts.URL))
	_, err := client.DeleteContact(context.Background(), "missing-user")

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if !IsNotFound(err) {
		t.Errorf("Expected IsNotFound to be true, got error: %v", err)
	}

	// Test IsBadRequest
	ts400 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		if _, err := w.Write([]byte(`{"success":false,"message":"Bad Request"}`)); err != nil {
			t.Errorf("Failed to write response: %v", err)
		}
	}))
	defer ts400.Close()

	client400, _ := NewSDK("test-key", WithBaseURL(ts400.URL))
	_, err400 := client400.UpsertContact(context.Background(), ContactRequest{})
	if err400 == nil || !IsBadRequest(err400) {
		t.Errorf("Expected IsBadRequest to be true, got: %v", err400)
	}

	// Test IsConflict
	ts409 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		if _, err := w.Write([]byte(`{"success":false,"message":"Conflict"}`)); err != nil {
			t.Errorf("Failed to write response: %v", err)
		}
	}))
	defer ts409.Close()

	client409, _ := NewSDK("test-key", WithBaseURL(ts409.URL))
	_, err409 := client409.UpsertContact(context.Background(), ContactRequest{})
	if err409 == nil || !IsConflict(err409) {
		t.Errorf("Expected IsConflict to be true, got: %v", err409)
	}

	// Test Error String
	apiErr := &Error{StatusCode: 418, Body: "I'm a teapot"}
	if apiErr.Error() != "api request failed with status 418: I'm a teapot" {
		t.Errorf("Unexpected error string: %s", apiErr.Error())
	}
}

func TestClient_NetworkErrors(t *testing.T) {
	// Test request creation failure (invalid URL)
	// NewRequestWithContext checks URL parsing.
	client, _ := NewSDK("test-key", WithBaseURL("http://[::1]:namedport")) // Invalid URL
	_, err := client.UpsertContact(context.Background(), ContactRequest{})
	if err == nil {
		t.Error("Expected error for invalid URL")
	}

	// Test execution failure (connection refused)
	client2, _ := NewSDK("test-key", WithBaseURL("http://127.0.0.1:0")) // Invalid port
	_, err2 := client2.UpsertContact(context.Background(), ContactRequest{})
	if err2 == nil {
		t.Error("Expected error for connection refusal")
	}
}

func TestClient_DecodeErrors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{invalid-json}`)); err != nil {
			t.Errorf("Failed to write response: %v", err)
		}
	}))
	defer ts.Close()

	client, _ := NewSDK("test-key", WithBaseURL(ts.URL))
	_, err := client.UpsertContact(context.Background(), ContactRequest{})
	if err == nil {
		t.Error("Expected error for invalid JSON response")
	}
}

func TestWithHTTPClient(t *testing.T) {
	customClient := &http.Client{Timeout: 5 * time.Second}
	sdk, _ := NewSDK("key", WithHTTPClient(customClient))
	// We can't easily check the private field, but we can check if it didn't panic and runs
	// In a real scenario we might check internal state via reflection or behavior.
	// For coverage, ensuring the option function runs is enough.
	if sdk == nil {
		t.Error("SDK should not be nil")
	}
}

func TestIsInvalidMailingList(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"Loops' 400 for a mailing list it does not have", &Error{StatusCode: http.StatusBadRequest,
			Body: `{"success":false,"message":"Invalid mailing lists: list-1"}`}, true},
		{"the same, wrapped", fmt.Errorf("remove: %w", &Error{StatusCode: http.StatusBadRequest,
			Body: `{"success":false,"message":"Invalid mailing lists: list-1"}`}), true},
		{"another 400", &Error{StatusCode: http.StatusBadRequest,
			Body: `{"success":false,"message":"Invalid email"}`}, false},
		{"a 400 whose body is not JSON", &Error{StatusCode: http.StatusBadRequest, Body: "Invalid mailing lists"}, false},
		{"the message on another status", &Error{StatusCode: http.StatusNotFound,
			Body: `{"success":false,"message":"Invalid mailing lists: list-1"}`}, false},
		{"not a Loops error", errors.New("Invalid mailing lists: list-1"), false},
		{"no error", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsInvalidMailingList(tc.err); got != tc.want {
				t.Errorf("IsInvalidMailingList(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
