package loops

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// invalidMailingListsMessage starts the message of Loops' 400 for a request that names a mailing list Loops does
// not have: "Invalid mailing lists: <id>".
const invalidMailingListsMessage = "Invalid mailing lists"

// contactNotFoundMessage is the message of Loops' 400 for a contact it does not hold.
const contactNotFoundMessage = "Contact not found"

// Error represents an error returned by the Loops API.
type Error struct {
	StatusCode int
	Body       string
}

func (e *Error) Error() string {
	return fmt.Sprintf("api request failed with status %d: %s", e.StatusCode, e.Body)
}

// IsErrorStatus checks if the error is a Loops API error with the given status code.
func isErrorStatus(err error, status int) bool {
	var apiErr *Error
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == status
	}
	return false
}

// IsBadRequest checks if the error represents a 400 Bad Request response.
func IsBadRequest(err error) bool {
	return isErrorStatus(err, http.StatusBadRequest)
}

// IsNotFound checks if the error represents a 404 Not Found response.
func IsNotFound(err error) bool {
	return isErrorStatus(err, http.StatusNotFound)
}

// badRequestMessage returns the message of Loops' 400, and false for any other error or a body that is not JSON.
func badRequestMessage(err error) (string, bool) {
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadRequest {
		return "", false
	}
	var body struct {
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(apiErr.Body), &body) != nil {
		return "", false
	}
	return body.Message, true
}

// IsInvalidMailingList checks if the error is Loops' 400 for a mailing list it does not have.
// Any other 400, such as an invalid email address, returns false.
func IsInvalidMailingList(err error) bool {
	message, ok := badRequestMessage(err)
	return ok && strings.HasPrefix(message, invalidMailingListsMessage)
}

// IsContactNotFound checks if the error is Loops' 400 for a contact it does not hold, as it answers an update that
// names the contact by user ID alone. Any other 400 returns false.
func IsContactNotFound(err error) bool {
	message, ok := badRequestMessage(err)
	return ok && message == contactNotFoundMessage
}

// IsConflict checks if the error represents a 409 Conflict response.
func IsConflict(err error) bool {
	return isErrorStatus(err, http.StatusConflict)
}
