package webhook

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// eventsTotal counts Loops' deliveries by event name and by what the receiver did with them.
var eventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "loops_webhook_events_total",
	Help: "Deliveries from Loops, by event name and by what the receiver did with them.",
}, []string{"event", "outcome"})

func init() {
	metrics.Registry.MustRegister(eventsTotal)
}
