package eventhandler

import (
	"context"
	"log"

	"github.com/diijam/cloud_functions/exporter"
)

type pubSubMessage struct {
	Data []byte `json:"data"`
}

// ExporterEventHandler handles cloud scheduler events
func ExporterEventHandler(ctx context.Context, m pubSubMessage) error {
	log.Print("Starting export Firebase events from BigQuery to GCS")
	return exporter.ExportYesterdayFirebaseTableAsJSON()
}
