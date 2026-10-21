package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	notificationv1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

var testSecret = "whsec_" + base64.StdEncoding.EncodeToString([]byte("test-signing-key"))

// actAt is when the fixtures' Removal was made and the time the event cases are set around.
var actAt = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

// loopsEvent is the body Loops posts. An empty userID or listID is sent as null, as Loops does for a contact that
// has no userId.
type loopsEvent struct {
	name   string
	at     time.Time
	userID string
	listID string
}

func (e loopsEvent) body() []byte {
	event := map[string]any{
		"eventName":            e.name,
		"eventTime":            e.at.Unix(),
		"webhookSchemaVersion": "1.0.0",
		"contactIdentity":      map[string]any{"id": "loops-contact-id", "email": "alice@example.com", "userId": orNull(e.userID)},
	}
	if strings.HasPrefix(e.name, "contact.mailingList.") {
		event["mailingList"] = map[string]any{"id": orNull(e.listID), "name": "Newsletter", "isPublic": true}
	}
	body, _ := json.Marshal(event)
	return body
}

func orNull(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func listEvent(name string, at time.Time) loopsEvent {
	return loopsEvent{name: name, at: at, userID: "alice-uid", listID: "list-1"}
}

func contactEvent(name string, at time.Time) loopsEvent {
	return loopsEvent{name: name, at: at, userID: "alice-uid"}
}

// sign returns Loops' Webhook-Signature for one delivery: v1, then the base64 HMAC-SHA256 of "id.timestamp.body".
func sign(secret, id string, at time.Time, body []byte) string {
	key, _ := base64.StdEncoding.DecodeString(strings.SplitN(secret, "_", 2)[1])
	mac := hmac.New(sha256.New, key)
	_, _ = fmt.Fprintf(mac, "%s.%d.%s", id, at.Unix(), body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// delivery is one POST as Loops sends it.
type delivery struct {
	id       string
	sentAt   time.Time
	body     []byte
	signedAs []byte          // the body the signature covers; nil means body
	secret   string          // the secret Loops signs with; empty means testSecret
	ctx      context.Context // the request's context; nil means a background one
}

func deliverOnce(wh *Webhook, e loopsEvent, id string) int {
	return deliver(wh, delivery{id: id, sentAt: time.Now(), body: e.body()})
}

func deliver(wh *Webhook, d delivery) int {
	signed, secret := d.signedAs, d.secret
	if signed == nil {
		signed = d.body
	}
	if secret == "" {
		secret = testSecret
	}
	req := httptest.NewRequest(http.MethodPost, wh.Endpoint, bytes.NewReader(d.body))
	if d.ctx != nil {
		req = req.WithContext(d.ctx)
	}
	req.Header.Set("webhook-id", d.id)
	req.Header.Set("webhook-timestamp", strconv.FormatInt(d.sentAt.Unix(), 10))
	req.Header.Set("webhook-signature", sign(secret, d.id, d.sentAt, signed))
	rec := httptest.NewRecorder()
	wh.ServeHTTP(rec, req)
	return rec.Code
}

func alice() *notificationv1.Contact {
	return &notificationv1.Contact{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default", UID: types.UID("alice-uid"), Generation: 3},
		Spec:       notificationv1.ContactSpec{GivenName: "Alice", FamilyName: "Doe", Email: "alice@example.com"},
	}
}

func newsletter() *notificationv1.ContactGroup {
	return &notificationv1.ContactGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "newsletter", Namespace: "default"},
		Spec: notificationv1.ContactGroupSpec{
			DisplayName: "Newsletter",
			Visibility:  notificationv1.ContactGroupVisibilityPublic,
			Providers:   []notificationv1.ContactGroupProviderSpec{{Name: "Loops", ID: "list-1"}},
		},
	}
}

func aliceInNewsletter() *notificationv1.ContactGroupMembership {
	return &notificationv1.ContactGroupMembership{
		ObjectMeta: metav1.ObjectMeta{Name: "alice-newsletter", Namespace: "default"},
		Spec: notificationv1.ContactGroupMembershipSpec{
			ContactRef:      notificationv1.ContactReference{Name: "alice", Namespace: "default"},
			ContactGroupRef: notificationv1.ContactGroupReference{Name: "newsletter", Namespace: "default"},
		},
	}
}

func aliceOptedOut(at time.Time) *notificationv1.ContactGroupMembershipRemoval {
	return &notificationv1.ContactGroupMembershipRemoval{
		ObjectMeta: metav1.ObjectMeta{Name: "alice-out", Namespace: "default", CreationTimestamp: metav1.NewTime(at)},
		Spec: notificationv1.ContactGroupMembershipRemovalSpec{
			ContactRef:      notificationv1.ContactReference{Name: "alice", Namespace: "default"},
			ContactGroupRef: notificationv1.ContactGroupReference{Name: "newsletter", Namespace: "default"},
		},
	}
}

// newReceiver returns the webhook over a fake Milo that holds objs and has the manager's indexes.
func newReceiver(t *testing.T, funcs *interceptor.Funcs, objs ...client.Object) (*Webhook, client.Client) {
	t.Helper()
	sch := runtime.NewScheme()
	if err := notificationv1.AddToScheme(sch); err != nil {
		t.Fatalf("failed to register notification types in scheme: %v", err)
	}
	builder := fake.NewClientBuilder().WithScheme(sch).WithObjects(objs...).
		WithStatusSubresource(&notificationv1.Contact{})
	for _, index := range fieldIndexes {
		builder = builder.WithIndex(index.object, index.field, index.extract)
	}
	if funcs != nil {
		builder = builder.WithInterceptorFuncs(*funcs)
	}
	c := builder.Build()
	return NewLoopsContactGroupMembershipWebhookV1(c, testSecret), c
}

func memberships(t *testing.T, c client.Client) []notificationv1.ContactGroupMembership {
	t.Helper()
	var list notificationv1.ContactGroupMembershipList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("list memberships: %v", err)
	}
	return list.Items
}

func removals(t *testing.T, c client.Client) []notificationv1.ContactGroupMembershipRemoval {
	t.Helper()
	var list notificationv1.ContactGroupMembershipRemovalList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("list removals: %v", err)
	}
	return list.Items
}

func contactCondition(t *testing.T, c client.Client, conditionType string) *metav1.Condition {
	t.Helper()
	contact := &notificationv1.Contact{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "alice", Namespace: "default"}, contact); err != nil {
		t.Fatalf("get contact: %v", err)
	}
	return meta.FindStatusCondition(contact.Status.Conditions, conditionType)
}

// duplicateRefusal is Milo's admission answer to a second membership or Removal for one contact and group.
func duplicateRefusal(kind string) error {
	return apierrors.NewInvalid(schema.GroupKind{Group: "notification.miloapis.com", Kind: kind}, "dup",
		field.ErrorList{field.Duplicate(field.NewPath("spec"), "alice/newsletter")})
}

// M2: an event the receiver cannot act on gets a 2xx, so Loops does not retry it for 28 hours.
func TestReceiver_AnswersTwoHundredToWhatItCannotActOn(t *testing.T) {
	cases := map[string]loopsEvent{
		"unhandled type":  contactEvent("contact.created", actAt),
		"no userId":       {name: "contact.mailingList.unsubscribed", at: actAt, listID: "list-1"},
		"unknown contact": {name: "contact.mailingList.unsubscribed", at: actAt, userID: "someone-else", listID: "list-1"},
		"no list ID":      {name: "contact.mailingList.unsubscribed", at: actAt, userID: "alice-uid"},
		"unknown list":    {name: "contact.mailingList.unsubscribed", at: actAt, userID: "alice-uid", listID: "list-9"},
		"contact.deleted for a contact Milo does not hold": {name: "contact.deleted", at: actAt, userID: "someone-else"},
	}
	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			wh, c := newReceiver(t, nil, alice(), newsletter(), aliceInNewsletter())
			if code := deliverOnce(wh, event, "msg-1"); code != http.StatusOK {
				t.Errorf("answered %d, want 200", code)
			}
			if got := removals(t, c); len(got) != 0 {
				t.Errorf("made %d Removals, want none", len(got))
			}
		})
	}
}

// M3: our own add comes back as mailingList.subscribed. The person is already a member, so it is done.
func TestReceiver_OurOwnAddIsDone(t *testing.T) {
	t.Run("a live membership", func(t *testing.T) {
		wh, c := newReceiver(t, nil, alice(), newsletter(), aliceInNewsletter())
		if code := deliverOnce(wh, listEvent("contact.mailingList.subscribed", actAt), "msg-1"); code != http.StatusOK {
			t.Errorf("answered %d, want 200", code)
		}
		if got := memberships(t, c); len(got) != 1 {
			t.Errorf("holds %d memberships, want 1", len(got))
		}
	})
	t.Run("Milo refuses the create as a duplicate", func(t *testing.T) {
		refuse := &interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*notificationv1.ContactGroupMembership); ok {
				return duplicateRefusal("ContactGroupMembership")
			}
			return c.Create(ctx, obj, opts...)
		}}
		wh, _ := newReceiver(t, refuse, alice(), newsletter())
		if code := deliverOnce(wh, listEvent("contact.mailingList.subscribed", actAt), "msg-1"); code != http.StatusOK {
			t.Errorf("answered %d, want 200", code)
		}
	})
}

// M4: a subscribe applies only when Loops made it after the opt-out; a tie goes to the opt-out.
func TestReceiver_AnOptOutWinsOverAnOlderSubscribe(t *testing.T) {
	cases := map[string]struct {
		at      time.Time
		applied bool
	}{
		"a second older":  {actAt.Add(-time.Second), false},
		"the same second": {actAt, false},
		"a second newer":  {actAt.Add(time.Second), true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			wh, c := newReceiver(t, nil, alice(), newsletter(), aliceOptedOut(actAt))
			if code := deliverOnce(wh, listEvent("contact.mailingList.subscribed", tc.at), "msg-1"); code != http.StatusOK {
				t.Errorf("answered %d, want 200", code)
			}
			removed, joined := len(removals(t, c)) == 0, len(memberships(t, c)) == 1
			if removed != tc.applied || joined != tc.applied {
				t.Errorf("Removal deleted %v, membership made %v; want both %v", removed, joined, tc.applied)
			}
		})
	}
}

// M5: a delivery handled with a 2xx is not handled twice.
func TestReceiver_DropsADeliveryItAlreadyHandled(t *testing.T) {
	wh, c := newReceiver(t, nil, alice(), newsletter(), aliceInNewsletter())
	event := listEvent("contact.mailingList.unsubscribed", actAt)
	if code := deliverOnce(wh, event, "msg-1"); code != http.StatusOK {
		t.Fatalf("first delivery answered %d, want 200", code)
	}
	for _, r := range removals(t, c) {
		if err := c.Delete(context.Background(), &r); err != nil {
			t.Fatalf("delete removal: %v", err)
		}
	}
	if code := deliverOnce(wh, event, "msg-1"); code != http.StatusOK {
		t.Errorf("second delivery answered %d, want 200", code)
	}
	if got := removals(t, c); len(got) != 0 {
		t.Errorf("the second delivery made %d Removals, want none", len(got))
	}
}

// M5: a delivery answered 5xx is handled when Loops retries it.
func TestReceiver_HandlesTheRetryOfAFailedDelivery(t *testing.T) {
	failures := 1
	failOnce := &interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if _, ok := obj.(*notificationv1.ContactGroupMembershipRemoval); ok && failures > 0 {
			failures--
			return apierrors.NewServiceUnavailable("milo is restarting")
		}
		return c.Create(ctx, obj, opts...)
	}}
	wh, c := newReceiver(t, failOnce, alice(), newsletter(), aliceInNewsletter())
	event := listEvent("contact.mailingList.unsubscribed", actAt)
	if code := deliverOnce(wh, event, "msg-1"); code < 500 {
		t.Fatalf("a failed write answered %d, want 5xx", code)
	}
	if code := deliverOnce(wh, event, "msg-1"); code != http.StatusOK {
		t.Errorf("the retry answered %d, want 200", code)
	}
	if got := removals(t, c); len(got) != 1 {
		t.Errorf("the retry made %d Removals, want 1", len(got))
	}
}

// M12: an unsubscribe for a membership Milo no longer holds, or is deleting, is the echo of our own removal.
func TestReceiver_OurOwnRemovalIsAnEcho(t *testing.T) {
	deleting := aliceInNewsletter()
	deleting.Finalizers = []string{"notification.miloapis.com/loops-contactgroupmembership"}
	deleting.DeletionTimestamp = &metav1.Time{Time: actAt}
	cases := map[string]struct {
		objs    []client.Object
		removal bool
	}{
		"no membership":              {[]client.Object{alice(), newsletter()}, false},
		"a membership being deleted": {[]client.Object{alice(), newsletter(), deleting}, false},
		"a live membership":          {[]client.Object{alice(), newsletter(), aliceInNewsletter()}, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			wh, c := newReceiver(t, nil, tc.objs...)
			if code := deliverOnce(wh, listEvent("contact.mailingList.unsubscribed", actAt), "msg-1"); code != http.StatusOK {
				t.Errorf("answered %d, want 200", code)
			}
			if got := len(removals(t, c)) == 1; got != tc.removal {
				t.Errorf("made a Removal %v, want %v", got, tc.removal)
			}
		})
	}
}

// M3, the Removal side: Milo refuses a second Removal for one contact and group, and that is done.
func TestReceiver_ADuplicateRemovalIsDone(t *testing.T) {
	refuse := &interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if _, ok := obj.(*notificationv1.ContactGroupMembershipRemoval); ok {
			return duplicateRefusal("ContactGroupMembershipRemoval")
		}
		return c.Create(ctx, obj, opts...)
	}}
	wh, _ := newReceiver(t, refuse, alice(), newsletter(), aliceInNewsletter())
	if code := deliverOnce(wh, listEvent("contact.mailingList.unsubscribed", actAt), "msg-1"); code != http.StatusOK {
		t.Errorf("answered %d, want 200", code)
	}
}

// M6, the write side: the receiver never answers 2xx before its write to Milo is done.
func TestReceiver_AFailedWriteIsRetriedByLoops(t *testing.T) {
	fail := &interceptor.Funcs{Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
		return apierrors.NewServiceUnavailable("milo is restarting")
	}}
	wh, _ := newReceiver(t, fail, alice(), newsletter(), aliceInNewsletter())
	if code := deliverOnce(wh, listEvent("contact.mailingList.unsubscribed", actAt), "msg-1"); code < 500 {
		t.Errorf("answered %d, want 5xx", code)
	}
}

// M8: contact.deleted marks the Contact missing in Loops, at the Contact's own generation.
func TestReceiver_RecordsAContactDeletedInLoops(t *testing.T) {
	wh, c := newReceiver(t, nil, alice())
	if code := deliverOnce(wh, contactEvent("contact.deleted", actAt), "msg-1"); code != http.StatusOK {
		t.Fatalf("answered %d, want 200", code)
	}
	cond := contactCondition(t, c, "LoopsContactReady")
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ContactMissingInLoops" {
		t.Fatalf("LoopsContactReady = %+v, want False ContactMissingInLoops", cond)
	}
	if cond.ObservedGeneration != 3 {
		t.Errorf("observedGeneration = %d, want the Contact's generation 3", cond.ObservedGeneration)
	}
}

// M9 and M11: Loops' audience state is recorded with the event's time, and only a newer event changes it.
func TestReceiver_RecordsTheAudienceStateInOrder(t *testing.T) {
	type step struct {
		name string
		at   time.Time
	}
	cases := map[string]struct {
		steps  []step
		status metav1.ConditionStatus
		reason string
	}{
		"an unsubscribe": {
			[]step{{"contact.unsubscribed", actAt}}, metav1.ConditionFalse, "UnsubscribedInLoops"},
		"a newer resubscribe": {
			[]step{{"contact.unsubscribed", actAt}, {"email.resubscribed", actAt.Add(time.Second)}},
			metav1.ConditionTrue, "ResubscribedInLoops"},
		"an older resubscribe": {
			[]step{{"contact.unsubscribed", actAt}, {"email.resubscribed", actAt.Add(-time.Second)}},
			metav1.ConditionFalse, "UnsubscribedInLoops"},
		"an older unsubscribe": {
			[]step{{"email.resubscribed", actAt}, {"contact.unsubscribed", actAt.Add(-time.Second)}},
			metav1.ConditionTrue, "ResubscribedInLoops"},
		"an unsubscribe in the same second as a resubscribe": {
			[]step{{"email.resubscribed", actAt}, {"contact.unsubscribed", actAt}},
			metav1.ConditionFalse, "UnsubscribedInLoops"},
		"a resubscribe in the same second as an unsubscribe": {
			[]step{{"contact.unsubscribed", actAt}, {"email.resubscribed", actAt}},
			metav1.ConditionFalse, "UnsubscribedInLoops"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			wh, c := newReceiver(t, nil, alice())
			for i, s := range tc.steps {
				if code := deliverOnce(wh, contactEvent(s.name, s.at), fmt.Sprintf("msg-%d", i)); code != http.StatusOK {
					t.Fatalf("%s answered %d, want 200", s.name, code)
				}
			}
			cond := contactCondition(t, c, "LoopsSubscribed")
			if cond == nil || cond.Status != tc.status || cond.Reason != tc.reason {
				t.Fatalf("LoopsSubscribed = %+v, want %s %s", cond, tc.status, tc.reason)
			}
		})
	}
}

// group returns a ContactGroup with `visibility`, served by the Loops list `listID` when it is not empty.
func group(name string, visibility notificationv1.ContactGroupVisibility, listID string) *notificationv1.ContactGroup {
	g := &notificationv1.ContactGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       notificationv1.ContactGroupSpec{DisplayName: name, Visibility: visibility},
	}
	if listID != "" {
		g.Spec.Providers = []notificationv1.ContactGroupProviderSpec{{Name: "Loops", ID: listID}}
	}
	return g
}

func aliceIn(groupName string) *notificationv1.ContactGroupMembership {
	m := aliceInNewsletter()
	m.Name, m.Spec.ContactGroupRef.Name = "alice-"+groupName, groupName
	return m
}

// Viktor's rule (2026-10-21): a global unsubscribe takes the person out of a public group whose Loops list carries
// marketing, with a Removal; a private group keeps the membership, and only LoopsSubscribed records the state.
func TestReceiver_AGlobalUnsubscribeLeavesOnlyThePublicLoopsGroups(t *testing.T) {
	public := group("newsletter", notificationv1.ContactGroupVisibilityPublic, "list-1")
	private := group("all-users", notificationv1.ContactGroupVisibilityPrivate, "list-2")
	noLoops := group("investors", notificationv1.ContactGroupVisibilityPublic, "")
	cases := map[string]struct {
		objs []client.Object
		want []string // the groups that get a Removal
	}{
		"a public Loops group":         {[]client.Object{alice(), public, aliceIn("newsletter")}, []string{"newsletter"}},
		"a private Loops group":        {[]client.Object{alice(), private, aliceIn("all-users")}, nil},
		"a public group without Loops": {[]client.Object{alice(), noLoops, aliceIn("investors")}, nil},
		"a public and a private group": {[]client.Object{alice(), public, private, aliceIn("newsletter"),
			aliceIn("all-users")}, []string{"newsletter"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			wh, c := newReceiver(t, nil, tc.objs...)
			if code := deliverOnce(wh, contactEvent("contact.unsubscribed", actAt), "msg-1"); code != http.StatusOK {
				t.Fatalf("answered %d, want 200", code)
			}
			if got := removedGroups(t, c); fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("Removals for %v, want %v", got, tc.want)
			}
			if cond := contactCondition(t, c, "LoopsSubscribed"); cond == nil || cond.Status != metav1.ConditionFalse {
				t.Errorf("LoopsSubscribed = %+v, want False", cond)
			}
		})
	}
}

// An unsubscribe older than the recorded resubscribe changes nothing, the Removals included.
func TestReceiver_AnOutOfOrderGlobalUnsubscribeMakesNoRemoval(t *testing.T) {
	wh, c := newReceiver(t, nil, alice(), newsletter(), aliceInNewsletter())
	deliverOnce(wh, contactEvent("email.resubscribed", actAt), "msg-1")
	if code := deliverOnce(wh, contactEvent("contact.unsubscribed", actAt.Add(-time.Second)), "msg-2"); code != http.StatusOK {
		t.Fatalf("answered %d, want 200", code)
	}
	if got := removedGroups(t, c); len(got) != 0 {
		t.Errorf("Removals for %v, want none", got)
	}
}

func removedGroups(t *testing.T, c client.Client) []string {
	t.Helper()
	list := removals(t, c)
	names := make([]string, 0, len(list))
	for _, r := range list {
		names = append(names, r.Spec.ContactGroupRef.Name)
	}
	return names
}

// M9: the unsubscribed message does not claim more than Loops says.
func TestReceiver_TheUnsubscribeMessageNamesItsOtherCauses(t *testing.T) {
	wh, c := newReceiver(t, nil, alice())
	if code := deliverOnce(wh, contactEvent("contact.unsubscribed", actAt), "msg-1"); code != http.StatusOK {
		t.Fatalf("answered %d, want 200", code)
	}
	cond := contactCondition(t, c, "LoopsSubscribed")
	if cond == nil {
		t.Fatal("no LoopsSubscribed condition")
	}
	for _, want := range []string{actAt.Format(time.RFC3339), "hard bounce", "deletion"} {
		if !strings.Contains(cond.Message, want) {
			t.Errorf("message %q does not name %q", cond.Message, want)
		}
	}
	if !cond.LastTransitionTime.Time.Equal(actAt) {
		t.Errorf("lastTransitionTime = %s, want the event's time %s", cond.LastTransitionTime, actAt)
	}
}

// M10: only Loops can deliver, and only recently.
func TestReceiver_RefusesADeliveryLoopsDidNotSignNow(t *testing.T) {
	body := listEvent("contact.mailingList.unsubscribed", actAt).body()
	cases := map[string]delivery{
		"a tampered body":    {id: "msg-1", sentAt: time.Now(), body: body, signedAs: []byte(`{"eventName":"testing.testEvent"}`)},
		"a wrong secret":     {id: "msg-1", sentAt: time.Now(), body: body, secret: "whsec_" + base64.StdEncoding.EncodeToString([]byte("other"))},
		"an old timestamp":   {id: "msg-1", sentAt: time.Now().Add(-10 * time.Minute), body: body},
		"a future timestamp": {id: "msg-1", sentAt: time.Now().Add(10 * time.Minute), body: body},
	}
	for name, d := range cases {
		t.Run(name, func(t *testing.T) {
			wh, c := newReceiver(t, nil, alice(), newsletter(), aliceInNewsletter())
			if code := deliver(wh, d); code != http.StatusUnauthorized {
				t.Errorf("answered %d, want 401", code)
			}
			if got := removals(t, c); len(got) != 0 {
				t.Errorf("made %d Removals, want none", len(got))
			}
		})
	}
}
