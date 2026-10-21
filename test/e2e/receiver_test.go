//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"go.miloapis.com/email-provider-loops/test/utils"
)

// e2eSigningSecret is the signing secret the suite gives the receiver, in Loops' format: a prefix, then the
// base64 key. The specs below sign every event they post with it.
var e2eSigningSecret = "whsec_" + base64.StdEncoding.EncodeToString([]byte("e2e-signing-key"))

const (
	receiverDeployment   = "email-provider-loops-webhook"
	receiverService      = "email-provider-loops-webhook"
	managerDeployment    = "email-provider-loops-controller-manager"
	receiverPath         = "/apis/emailnotification.k8s.io/v1/loops/contactgroupmemberships"
	forwardedPort        = "18090"
	forwardedMetricsPort = "18443"
	e2eListID            = "e2e-list"
)

// receiverFixtures is a Contact, a public and a private group each served by a Loops list, and the Contact's
// membership of each.
const receiverFixtures = `
apiVersion: notification.miloapis.com/v1alpha1
kind: Contact
metadata:
  name: e2e-alice
  namespace: default
spec:
  givenName: Alice
  familyName: Doe
  email: alice@example.com
---
apiVersion: notification.miloapis.com/v1alpha1
kind: ContactGroup
metadata:
  name: e2e-newsletter
  namespace: default
spec:
  displayName: E2E Newsletter
  visibility: public
  providers:
  - name: Loops
    id: ` + e2eListID + `
---
apiVersion: notification.miloapis.com/v1alpha1
kind: ContactGroupMembership
metadata:
  name: e2e-alice-newsletter
  namespace: default
spec:
  contactRef:
    name: e2e-alice
    namespace: default
  contactGroupRef:
    name: e2e-newsletter
    namespace: default
---
apiVersion: notification.miloapis.com/v1alpha1
kind: ContactGroup
metadata:
  name: e2e-all-users
  namespace: default
spec:
  displayName: E2E All Users
  visibility: private
  providers:
  - name: Loops
    id: e2e-private-list
---
apiVersion: notification.miloapis.com/v1alpha1
kind: ContactGroupMembership
metadata:
  name: e2e-alice-all-users
  namespace: default
spec:
  contactRef:
    name: e2e-alice
    namespace: default
  contactGroupRef:
    name: e2e-all-users
    namespace: default
`

// receiverSpecs posts events signed as Loops signs them to the receiver's Service, and reads what they made in the
// cluster's Milo types. The controller manager is stopped meanwhile: it would call Loops for the fixtures' Contact, and
// these specs read the receiver alone.
func receiverSpecs() {
	Context("Loops webhook receiver", Ordered, func() {
		var forward *exec.Cmd
		var userID string

		BeforeAll(func() {
			By("stopping the controller manager")
			kubectl("scale", "deployment", managerDeployment, "-n", namespace, "--replicas=0")
			By("waiting for the receiver to serve with its certificate")
			kubectl("rollout", "status", "deployment/"+receiverDeployment, "-n", namespace, "--timeout=3m")
			By("creating the fixtures")
			kubectlStdin(receiverFixtures, "apply", "-f", "-")
			userID = kubectl("get", "contacts.notification.miloapis.com", "e2e-alice", "-n", "default",
				"-o", "jsonpath={.metadata.uid}")
			By("forwarding a local port to the receiver's Service")
			forward = exec.Command("kubectl", "port-forward", "-n", namespace, "svc/"+receiverService,
				forwardedPort+":8090")
			Expect(forward.Start()).To(Succeed())
			Eventually(receiverAnswersGet).Should(Equal(http.StatusMethodNotAllowed))
		})

		AfterAll(func() {
			if forward != nil && forward.Process != nil {
				_ = forward.Process.Kill()
			}
			cmd := exec.Command("kubectl", "delete", "--ignore-not-found", "-f", "-")
			cmd.Stdin = strings.NewReader(receiverFixtures)
			_, _ = utils.Run(cmd)
			_, _ = utils.Run(exec.Command("kubectl", "delete", "contactgroupmembershipremovals.notification.miloapis.com",
				"--all", "-n", "default", "--ignore-not-found"))
			_, _ = utils.Run(exec.Command("kubectl", "scale", "deployment", managerDeployment, "-n", namespace,
				"--replicas=1"))
		})

		It("records an unsubscribe from the list as a Removal", func() {
			// The receiver reads Milo through its cache, which may not hold the fixtures yet: until it does, the
			// event names an unknown contact, which is a 2xx and no Removal. A repeat after that is a no-op.
			Eventually(func(g Gomega) {
				g.Expect(post(listEvent("contact.mailingList.unsubscribed", userID, time.Now()))).To(Equal(http.StatusOK))
				g.Expect(removals()).To(Equal(1))
			}).Should(Succeed())
		})

		It("drops a subscribe that Loops saw before the opt-out", func() {
			Expect(post(listEvent("contact.mailingList.subscribed", userID, time.Now().Add(-time.Hour)))).
				To(Equal(http.StatusOK))
			Expect(removals()).To(Equal(1))
		})

		It("undoes the opt-out for a subscribe that Loops saw after it", func() {
			Expect(post(listEvent("contact.mailingList.subscribed", userID, time.Now().Add(2*time.Second)))).
				To(Equal(http.StatusOK))
			Expect(removals()).To(Equal(0))
			Expect(memberships()).To(Equal(2))
		})

		It("answers 2xx to what it cannot act on, and changes nothing", func() {
			Expect(post(listEvent("contact.mailingList.unsubscribed", "nobody", time.Now()))).To(Equal(http.StatusOK))
			Expect(post(contactEvent("contact.created", userID, time.Now()))).To(Equal(http.StatusOK))
			Expect(removals()).To(Equal(0))
		})

		It("takes an audience-wide unsubscribe out of the public group only, and records it on the Contact", func() {
			Expect(post(contactEvent("contact.unsubscribed", userID, time.Now()))).To(Equal(http.StatusOK))
			Expect(contactCondition("LoopsSubscribed")).To(Equal("False UnsubscribedInLoops"))
			Expect(removalGroups()).To(Equal("e2e-newsletter"))
			Expect(memberships()).To(Equal(2))
		})

		It("records a contact deleted in Loops on the Contact", func() {
			Expect(post(contactEvent("contact.deleted", userID, time.Now()))).To(Equal(http.StatusOK))
			Expect(contactCondition("LoopsContactReady")).To(Equal("False ContactMissingInLoops"))
		})

		It("serves plain-HTTP metrics that count each delivery by outcome", func() {
			metrics := exec.Command("kubectl", "port-forward", "-n", namespace, "svc/"+receiverService,
				forwardedMetricsPort+":8443")
			Expect(metrics.Start()).To(Succeed())
			defer func() { _ = metrics.Process.Kill() }()
			Eventually(func(g Gomega) {
				resp, err := http.Get("http://localhost:" + forwardedMetricsPort + "/metrics")
				g.Expect(err).NotTo(HaveOccurred())
				defer func() { _ = resp.Body.Close() }()
				body, err := io.ReadAll(resp.Body)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(string(body)).To(ContainSubstring(
					`loops_webhook_events_total{event="contact.mailingList.unsubscribed",outcome="applied"} 1`))
			}).Should(Succeed())
		})

		It("refuses an event that Loops did not sign", func() {
			body := contactEvent("contact.deleted", userID, time.Now())
			otherKey := "whsec_" + base64.StdEncoding.EncodeToString([]byte("another-key"))
			Expect(deliver(body, otherKey, time.Now())).To(Equal(http.StatusUnauthorized))
		})
	})
}

func kubectl(args ...string) string {
	out, err := utils.Run(exec.Command("kubectl", args...))
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return strings.TrimSpace(out)
}

func kubectlStdin(stdin string, args ...string) {
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = strings.NewReader(stdin)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
}

func count(resource string) int {
	out := kubectl("get", resource, "-n", "default", "-o", "jsonpath={.items[*].metadata.name}")
	return len(strings.Fields(out))
}

func removals() int { return count("contactgroupmembershipremovals.notification.miloapis.com") }

// removalGroups returns the groups the Removals in default name, space-separated.
func removalGroups() string {
	return kubectl("get", "contactgroupmembershipremovals.notification.miloapis.com", "-n", "default", "-o",
		"jsonpath={.items[*].spec.contactGroupRef.name}")
}
func memberships() int { return count("contactgroupmemberships.notification.miloapis.com") }

// contactCondition returns "<status> <reason>" of one condition on the fixtures' Contact.
func contactCondition(conditionType string) string {
	return kubectl("get", "contacts.notification.miloapis.com", "e2e-alice", "-n", "default", "-o",
		fmt.Sprintf(`jsonpath={.status.conditions[?(@.type=="%s")].status} {.status.conditions[?(@.type=="%s")].reason}`,
			conditionType, conditionType))
}

func contactEvent(name, userID string, at time.Time) []byte {
	return eventBody(map[string]any{"eventName": name, "eventTime": at.Unix(), "webhookSchemaVersion": "1.0.0",
		"contactIdentity": map[string]any{"id": "e2e", "email": "alice@example.com", "userId": userID}})
}

func listEvent(name, userID string, at time.Time) []byte {
	return eventBody(map[string]any{"eventName": name, "eventTime": at.Unix(), "webhookSchemaVersion": "1.0.0",
		"contactIdentity": map[string]any{"id": "e2e", "email": "alice@example.com", "userId": userID},
		"mailingList":     map[string]any{"id": e2eListID, "name": "E2E Newsletter", "isPublic": true}})
}

func eventBody(event map[string]any) []byte {
	body, err := json.Marshal(event)
	ExpectWithOffset(2, err).NotTo(HaveOccurred())
	return body
}

func post(body []byte) int {
	return deliver(body, e2eSigningSecret, time.Now())
}

// deliver posts body as Loops does: a fresh Webhook-Id, the time it was sent, and the signature over both and the
// body, made with secret.
func deliver(body []byte, secret string, sentAt time.Time) int {
	id := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	timestamp := strconv.FormatInt(sentAt.Unix(), 10)
	key, err := base64.StdEncoding.DecodeString(strings.SplitN(secret, "_", 2)[1])
	ExpectWithOffset(2, err).NotTo(HaveOccurred())
	mac := hmac.New(sha256.New, key)
	_, _ = fmt.Fprintf(mac, "%s.%s.%s", id, timestamp, body)

	req, err := http.NewRequest(http.MethodPost, "https://localhost:"+forwardedPort+receiverPath, bytes.NewReader(body))
	ExpectWithOffset(2, err).NotTo(HaveOccurred())
	req.Header.Set("webhook-id", id)
	req.Header.Set("webhook-timestamp", timestamp)
	req.Header.Set("webhook-signature", "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	resp, err := receiverClient().Do(req)
	ExpectWithOffset(2, err).NotTo(HaveOccurred())
	_ = resp.Body.Close()
	return resp.StatusCode
}

// receiverAnswersGet returns the status of a GET, which the receiver refuses with 405 once it serves; 0 before.
func receiverAnswersGet() int {
	resp, err := receiverClient().Get("https://localhost:" + forwardedPort + receiverPath)
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// receiverClient trusts the suite's self-signed certificate, which names the Service, not localhost.
func receiverClient() *http.Client {
	return &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // nolint:gosec // the suite's own self-signed certificate
	}}
}
