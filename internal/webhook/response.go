package webhook

import (
	"net/http"
)

// Outcomes name what the receiver did with one delivery. Each delivery logs and counts exactly one.
const (
	outcomeApplied         = "applied"
	outcomeUnhandled       = "unhandled"
	outcomeNoUserID        = "no_user_id"
	outcomeUnknownContact  = "unknown_contact"
	outcomeNoListID        = "no_list_id"
	outcomeUnknownList     = "unknown_list"
	outcomeOlderThanOptOut = "older_than_opt_out"
	outcomeMemberExists    = "member_exists"
	outcomeAlreadyRemoved  = "already_removed"
	outcomeEcho            = "echo"
	outcomeOutOfOrder      = "out_of_order"
	outcomeDuplicate       = "duplicate"
	outcomeOverBudget      = "over_budget"
	outcomeFailed          = "failed"
	outcomeBadRequest      = "bad_request"
	outcomeBadSignature    = "bad_signature"
	outcomeWrongMethod     = "wrong_method"
)

// handledResponse is a 2xx: Loops retries anything else, so every event the receiver has finished with gets one,
// whether or not it changed anything in Milo.
func handledResponse(outcome string) Response {
	return Response{HttpStatus: http.StatusOK, Outcome: outcome}
}

func OkResponse() Response {
	return handledResponse(outcomeApplied)
}

func BadRequestResponse() Response {
	return Response{HttpStatus: http.StatusBadRequest, Outcome: outcomeBadRequest}
}

func MethodNotAllowedResponse() Response {
	return Response{HttpStatus: http.StatusMethodNotAllowed, Outcome: outcomeWrongMethod}
}

func InternalServerErrorResponse() Response {
	return Response{HttpStatus: http.StatusInternalServerError, Outcome: outcomeFailed}
}

func UnauthorizedResponse() Response {
	return Response{HttpStatus: http.StatusUnauthorized, Outcome: outcomeBadSignature}
}

// overBudgetResponse is the answer when the handler's Milo calls outlast its budget; Loops retries it.
func overBudgetResponse() Response {
	return Response{HttpStatus: http.StatusServiceUnavailable, Outcome: outcomeOverBudget}
}
