package webhook

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	notificationmiloapiscomv1alpha1 "go.miloapis.com/milo/pkg/apis/notification/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	webhook "go.miloapis.com/email-provider-loops/internal/webhook"
)

// NewAuthenticationWebhookCommand returns a cobra command that starts the UserDeactivation
// TokenReview webhook server.
func CreateWebhookCommand() *cobra.Command {
	var (
		webhookPort                                     int
		webhookCertDir, webhookCertFile, webhookKeyFile string
		metricsBindAddress                              string
		probeAddr                                       string
	)

	cmd := &cobra.Command{
		Use:   "webhook",
		Short: "Runs the Loops Contact Group Membership webhook server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			logf.SetLogger(zap.New(zap.JSONEncoder()))
			log := logf.Log.WithName("webhook")

			log.Info("Starting webhook server",
				"cert_dir", webhookCertDir,
				"cert_file", webhookCertFile,
				"key_file", webhookKeyFile,
				"webhook_port", webhookPort,
			)

			log.Info("Metrics bind address",
				"metrics-bind-address", metricsBindAddress,
				"health-probe-bind-address", probeAddr,
			)

			// Setup Kubernetes client config
			restConfig, err := k8sconfig.GetConfig()
			if err != nil {
				return fmt.Errorf("failed to get rest config: %w", err)
			}

			runtimeScheme := runtime.NewScheme()
			if err := notificationmiloapiscomv1alpha1.AddToScheme(runtimeScheme); err != nil {
				return fmt.Errorf("failed to add notificationmiloapiscomv1alpha1 scheme: %w", err)
			}

			log.Info("Creating manager")
			mgr, err := manager.New(restConfig, manager.Options{
				Scheme:                 runtimeScheme,
				HealthProbeBindAddress: probeAddr,
				Metrics: server.Options{
					BindAddress: metricsBindAddress,
				},
				WebhookServer: ctrlwebhook.NewServer(ctrlwebhook.Options{
					CertDir:  webhookCertDir,
					CertName: webhookCertFile,
					KeyName:  webhookKeyFile,
					Port:     webhookPort,
				}),
			})
			if err != nil {
				return fmt.Errorf("failed to create manager: %w", err)
			}

			if err := addProbes(mgr); err != nil {
				return err
			}

			log.Info("Loading signing secret")
			signingSecret := os.Getenv("LOOPS_SIGNING_SECRET")
			if signingSecret == "" {
				return fmt.Errorf("LOOPS_SIGNING_SECRET is required but not set")
			}

			log.Info("Setting up webhook")
			webhookv1 := webhook.NewLoopsContactGroupMembershipWebhookV1(mgr.GetClient(), signingSecret)
			if err := webhookv1.SetupWithManager(mgr); err != nil {
				return fmt.Errorf("failed to setup webhook: %w", err)
			}

			log.Info("Starting manager")
			return mgr.Start(cmd.Context())

		},
	}

	// Network & Kubernetes flags.
	cmd.Flags().IntVar(&webhookPort, "webhook-port", 9443, "Port for the webhook server")
	cmd.Flags().StringVar(&webhookCertDir,
		"cert-dir", "/etc/certs", "Directory that contains the TLS certs to use for serving the webhook")
	cmd.Flags().StringVar(&webhookCertFile, "cert-file", "", "Filename in the directory that contains the TLS cert")
	cmd.Flags().StringVar(&webhookKeyFile, "key-file", "", "Filename in the directory that contains the TLS private key")

	// Metrics flags.
	cmd.Flags().StringVar(&metricsBindAddress, "metrics-bind-address", ":8080", "address the metrics endpoint binds to")

	cmd.Flags().StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")

	return cmd
}

func addProbes(mgr manager.Manager) error {
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("failed to set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("webhook", mgr.GetWebhookServer().StartedChecker()); err != nil {
		return fmt.Errorf("failed to set up ready check: %w", err)
	}
	return nil
}
