package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"go.miloapis.com/email-provider-loops/pkg/loops"
)

const (
	// handlerBudget is the time the handler's Milo calls get. Loops: "Your endpoint should respond within 15
	// seconds". An overrun answers 5xx, so Loops retries the event rather than losing it.
	handlerBudget = 10 * time.Second
	// signatureTolerance is how far a delivery's Webhook-Timestamp may be from now, either way, as in Loops' example
	// verifier (TOLERANCE_SECONDS = 300). It bounds the replay of a captured delivery.
	signatureTolerance = 5 * time.Minute
	// seenDeliveryTTL keeps a handled Webhook-Id as long as Loops retries: "Up to eight attempts are made over a
	// period of about 28 hours."
	seenDeliveryTTL = 28 * time.Hour
	// seenDeliveryLimit bounds that memory. A forgotten id is harmless: eventTime and Milo's admission keep a
	// repeated event from changing anything.
	seenDeliveryLimit = 10000
)

type Webhook struct {
	Handler       Handler
	Endpoint      string
	signingSecret string // Loops signing secret for webhook verification
	budget        time.Duration
	seen          *cache.LRUExpireCache // Webhook-Ids already answered with a 2xx
}

func newSeenDeliveries() *cache.LRUExpireCache {
	return cache.NewLRUExpireCache(seenDeliveryLimit)
}

type Request struct {
	MailingListSubscribedEvent   *loops.MailingListSubscribedEvent
	MailingListUnsubscribedEvent *loops.MailingListUnsubscribedEvent
	BaseEvent                    *loops.WebhookEvent
}

type Response struct {
	HttpStatus int    `json:"HttpStatus"`
	Outcome    string `json:"Outcome"`
}

type HandlerFunc func(context.Context, Request) Response

func (f HandlerFunc) Handle(ctx context.Context, req Request) Response {
	return f(ctx, req)
}

type Handler interface {
	Handle(context.Context, Request) Response
}

// WebhookVerificationError represents errors that can occur during webhook verification
type WebhookVerificationError struct {
	Code    string
	Message string
	Err     error
}

func (e *WebhookVerificationError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// Webhook verification error codes
var (
	ErrMissingHeaders     = errors.New("missing required webhook header")
	ErrMissingSecret      = errors.New("missing LOOPS_SIGNING_SECRET environment variable")
	ErrInvalidSignature   = errors.New("invalid signature")
	ErrStaleTimestamp     = errors.New("webhook timestamp outside the tolerance")
	ErrVerificationFailed = errors.New("webhook verification failed")
)

// verifyWebhook verifies the webhook signature from Loops, and that Loops sent it close to now
func verifyWebhook(r *http.Request, body []byte, secret string, now time.Time) error {
	eventID := r.Header.Get("webhook-id")
	timestamp := r.Header.Get("webhook-timestamp")
	webhookSignature := r.Header.Get("webhook-signature")
	if eventID == "" || timestamp == "" || webhookSignature == "" {
		return &WebhookVerificationError{
			Code:    "MISSING_HEADERS",
			Message: "Missing required webhook header",
			Err:     ErrMissingHeaders,
		}
	}
	if err := checkTimestamp(timestamp, now); err != nil {
		return err
	}
	key, err := signingKey(secret)
	if err != nil {
		return err
	}

	h := hmac.New(sha256.New, key)
	_, _ = fmt.Fprintf(h, "%s.%s.%s", eventID, timestamp, body)
	if !signatureMatches(webhookSignature, base64.StdEncoding.EncodeToString(h.Sum(nil))) {
		return &WebhookVerificationError{
			Code:    "INVALID_SIGNATURE",
			Message: "Invalid signature",
			Err:     ErrInvalidSignature,
		}
	}
	return nil
}

// checkTimestamp refuses a Webhook-Timestamp (seconds since epoch) more than signatureTolerance from now.
func checkTimestamp(timestamp string, now time.Time) error {
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return &WebhookVerificationError{Code: "INVALID_TIMESTAMP", Message: "Invalid webhook timestamp", Err: err}
	}
	if skew := now.Sub(time.Unix(seconds, 0)); skew > signatureTolerance || skew < -signatureTolerance {
		return &WebhookVerificationError{
			Code:    "STALE_TIMESTAMP",
			Message: fmt.Sprintf("Webhook timestamp is %s from now", skew.Round(time.Second)),
			Err:     ErrStaleTimestamp,
		}
	}
	return nil
}

// signingKey decodes the base64 key after the secret's prefix.
func signingKey(secret string) ([]byte, error) {
	parts := strings.Split(secret, "_")
	if len(parts) < 2 {
		return nil, &WebhookVerificationError{
			Code:    "INVALID_SECRET_FORMAT",
			Message: "Invalid LOOPS_SIGNING_SECRET format",
			Err:     ErrMissingSecret,
		}
	}
	key, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, &WebhookVerificationError{
			Code:    "INVALID_SECRET_ENCODING",
			Message: "Failed to decode LOOPS_SIGNING_SECRET",
			Err:     err,
		}
	}
	return key, nil
}

// signatureMatches compares expected, in constant time, with each "v1,<signature>" in the space-separated header.
func signatureMatches(header, expected string) bool {
	for _, sig := range strings.Split(header, " ") {
		if _, value, found := strings.Cut(sig, ","); found && hmac.Equal([]byte(value), []byte(expected)) {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the webhook with the Manager
func (w *Webhook) SetupWithManager(mgr ctrl.Manager) error {
	// Setup field indexes first
	if err := setupIndexes(mgr); err != nil {
		return err
	}

	hookServer := mgr.GetWebhookServer()
	hookServer.Register(w.Endpoint, w)

	return nil
}

func (wh *Webhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log := logf.FromContext(r.Context()).WithName("loops-http-webhook")
	eventName, response := "", InternalServerErrorResponse()
	defer func() {
		if p := recover(); p != nil {
			log.Error(nil, "Panic in webhook handler", "panic", p)
			response = InternalServerErrorResponse()
		}
		// One line and one count per delivery. The body is never logged: it carries the person's email address.
		log.Info("Handled Loops delivery", "eventName", eventName, "webhookId", r.Header.Get("webhook-id"),
			"outcome", response.Outcome, "status", response.HttpStatus)
		eventsTotal.WithLabelValues(eventName, response.Outcome).Inc()
		wh.writeResponse(w, response)
	}()

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		response = MethodNotAllowedResponse()
		return
	}
	eventName, response = wh.receive(r)
}

// receive verifies one delivery and hands its event to the handler once, within the budget.
func (wh *Webhook) receive(r *http.Request) (string, Response) {
	log := logf.FromContext(r.Context()).WithName("loops-http-webhook")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Error(err, "Failed to read request body")
		return "", InternalServerErrorResponse()
	}
	if err := verifyWebhook(r, body, wh.signingSecret, time.Now()); err != nil {
		log.Error(err, "Webhook verification failed")
		return "", UnauthorizedResponse()
	}
	var base loops.WebhookEvent
	if err := json.Unmarshal(body, &base); err != nil {
		log.Error(err, "Failed to parse base webhook event")
		return "", BadRequestResponse()
	}

	id := r.Header.Get("webhook-id")
	if _, seen := wh.seen.Get(id); seen {
		return base.EventName, handledResponse(outcomeDuplicate)
	}
	ctx, cancel := context.WithTimeout(r.Context(), wh.budget)
	defer cancel()
	response := wh.dispatch(ctx, body, &base)
	if response.HttpStatus >= http.StatusMultipleChoices && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		response = overBudgetResponse()
	}
	// Only a 2xx is remembered: Loops retries anything else, and the retry must be handled.
	if response.HttpStatus < http.StatusMultipleChoices {
		wh.seen.Add(id, struct{}{}, seenDeliveryTTL)
	}
	return base.EventName, response
}

// dispatch parses the event's own fields and hands it to the handler. An event type the receiver does not act on
// gets a 2xx, so Loops does not retry it.
func (wh *Webhook) dispatch(ctx context.Context, body []byte, base *loops.WebhookEvent) Response {
	req := Request{BaseEvent: base}
	switch base.EventName {
	case loops.EventNameMailingListSubscribed:
		req.MailingListSubscribedEvent = &loops.MailingListSubscribedEvent{}
		if err := json.Unmarshal(body, req.MailingListSubscribedEvent); err != nil {
			return BadRequestResponse()
		}
	case loops.EventNameMailingListUnsubscribed:
		req.MailingListUnsubscribedEvent = &loops.MailingListUnsubscribedEvent{}
		if err := json.Unmarshal(body, req.MailingListUnsubscribedEvent); err != nil {
			return BadRequestResponse()
		}
	case loops.EventNameContactDeleted, loops.EventNameContactUnsubscribed, loops.EventNameEmailResubscribed:
	default:
		return handledResponse(outcomeUnhandled)
	}
	return wh.Handler.Handle(ctx, req)
}

func (wh *Webhook) writeResponse(w http.ResponseWriter, response Response) {
	w.WriteHeader(response.HttpStatus)
}
