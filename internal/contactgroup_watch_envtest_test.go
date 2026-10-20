package controller

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	notificationv1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// miloNotificationCRDs returns the directory of Milo's notification CRDs at the Milo version go.mod requires.
func miloNotificationCRDs(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "go.miloapis.com/milo").Output()
	if err != nil || strings.TrimSpace(string(out)) == "" {
		t.Fatalf("find the go.miloapis.com/milo module: %v", err)
	}
	return filepath.Join(strings.TrimSpace(string(out)), "config", "crd", "bases", "notification")
}

// startMembershipManager runs the membership controller in a manager against a fresh API server, and returns a
// client of that server.
func startMembershipManager(t *testing.T, api *recordingLoops) client.Client {
	t.Helper()
	env := &envtest.Environment{CRDDirectoryPaths: []string{miloNotificationCRDs(t)}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start the API server: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	sch := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(sch)
	_ = notificationv1.AddToScheme(sch)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: sch, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatalf("create the manager: %v", err)
	}
	if err := (&LoopsContactGroupMembershipController{Client: mgr.GetClient(), Loops: api}).SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(ctx) }()

	c, err := client.New(cfg, client.Options{Scheme: sch})
	if err != nil {
		t.Fatalf("create a client: %v", err)
	}
	return c
}

func eventually(within time.Duration, done func() bool) bool {
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if done() {
			return true
		}
	}
	return false
}

func TestManager_AGroupThatAddsLoopsReachesItsMembershipAtOnce(t *testing.T) {
	ctx := context.Background()
	api := &recordingLoops{}
	c := startMembershipManager(t, api)

	group := &notificationv1.ContactGroup{ObjectMeta: metav1.ObjectMeta{Name: "devs", Namespace: "default"},
		Spec: notificationv1.ContactGroupSpec{DisplayName: "Devs", Visibility: notificationv1.ContactGroupVisibilityPrivate}}
	for _, obj := range []client.Object{
		&notificationv1.Contact{ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default"},
			Spec: notificationv1.ContactSpec{GivenName: "Alice", FamilyName: "Doe", Email: "alice@example.com"}},
		group,
		membershipOf("m-1", "default", "devs", "default"),
	} {
		if err := c.Create(ctx, obj); err != nil {
			t.Fatalf("create %s: %v", obj.GetName(), err)
		}
	}

	membership := &notificationv1.ContactGroupMembership{}
	if !eventually(20*time.Second, func() bool {
		return c.Get(ctx, client.ObjectKey{Name: "m-1", Namespace: "default"}, membership) == nil && len(membership.Finalizers) > 0
	}) {
		t.Fatal("the manager never reconciled the membership")
	}
	time.Sleep(3 * time.Second)
	if got := api.snapshot(); len(got) != 0 {
		t.Fatalf("Loops was called for a group that does not list it: %v", got)
	}

	if err := c.Get(ctx, client.ObjectKeyFromObject(group), group); err != nil {
		t.Fatalf("get the group: %v", err)
	}
	group.Spec.Providers = []notificationv1.ContactGroupProviderSpec{{Name: "Loops", ID: "list-1"}}
	if err := c.Update(ctx, group); err != nil {
		t.Fatalf("add Loops to the group: %v", err)
	}
	// The cache's resync is ten hours away, so only the group's watch can bring the membership back this soon.
	if !eventually(15*time.Second, func() bool {
		got := api.snapshot()
		return len(got) == 1 && strings.HasSuffix(got[0], "/list-1")
	}) {
		t.Fatalf("15 s after the group added Loops, its membership had not reached it: %v", api.snapshot())
	}
}
