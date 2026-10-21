package webhook

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// M6: Milo calls that outlast the budget answer 5xx at the budget, so Loops retries the event.
func TestReceiver_AnswersAtTheBudget(t *testing.T) {
	slowMilo := &interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return c.List(ctx, list, opts...)
		}
	}}
	wh, _ := newReceiver(t, slowMilo, alice(), newsletter(), aliceInNewsletter())
	wh.budget = 50 * time.Millisecond
	start := time.Now()
	code := deliverOnce(wh, listEvent("contact.mailingList.unsubscribed", actAt), "msg-1")
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("answered after %s, want about the 50ms budget", elapsed)
	}
	if code != http.StatusServiceUnavailable {
		t.Errorf("answered %d, want 503", code)
	}
}

// Each delivery is counted once, by event name and outcome.
func TestReceiver_CountsEachDeliveryByOutcome(t *testing.T) {
	wh, _ := newReceiver(t, nil, alice(), newsletter(), aliceInNewsletter())
	unhandled := eventsTotal.WithLabelValues("contact.created", outcomeUnhandled)
	applied := eventsTotal.WithLabelValues("contact.mailingList.unsubscribed", outcomeApplied)
	echo := eventsTotal.WithLabelValues("contact.mailingList.unsubscribed", outcomeAlreadyRemoved)
	before := []float64{testutil.ToFloat64(unhandled), testutil.ToFloat64(applied), testutil.ToFloat64(echo)}

	deliverOnce(wh, contactEvent("contact.created", actAt), "msg-1")
	deliverOnce(wh, listEvent("contact.mailingList.unsubscribed", actAt), "msg-2")
	deliverOnce(wh, listEvent("contact.mailingList.unsubscribed", actAt), "msg-3")

	after := []float64{testutil.ToFloat64(unhandled), testutil.ToFloat64(applied), testutil.ToFloat64(echo)}
	for i, name := range []string{"unhandled", "applied", "already_removed"} {
		if after[i]-before[i] != 1 {
			t.Errorf("%s counted %v times, want 1", name, after[i]-before[i])
		}
	}
}

// lineRecorder is a logger that keeps every line it is given.
type lineRecorder struct {
	mu    sync.Mutex
	lines []string
}

func (l *lineRecorder) logger() logr.Logger {
	return funcr.New(func(prefix, args string) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.lines = append(l.lines, prefix+" "+args)
	}, funcr.Options{Verbosity: 10})
}

// M13: the log names the delivery and never carries the person's email address.
func TestReceiver_LogsNoEmailAddress(t *testing.T) {
	cases := map[string]delivery{
		"a handled delivery": {id: "msg-1", sentAt: time.Now(), body: listEvent("contact.mailingList.unsubscribed", actAt).body()},
		"a refused delivery": {id: "msg-1", sentAt: time.Now().Add(-time.Hour), body: listEvent("contact.mailingList.unsubscribed", actAt).body()},
	}
	for name, d := range cases {
		t.Run(name, func(t *testing.T) {
			wh, _ := newReceiver(t, nil, alice(), newsletter(), aliceInNewsletter())
			rec := &lineRecorder{}
			d.ctx = logr.NewContext(context.Background(), rec.logger())
			deliver(wh, d)
			all := strings.Join(rec.lines, "\n")
			if strings.Contains(all, "alice@example.com") {
				t.Errorf("the log carries the email address:\n%s", all)
			}
			if !strings.Contains(all, `"webhookId"="msg-1"`) {
				t.Errorf("the log does not name the delivery:\n%s", all)
			}
		})
	}
}
