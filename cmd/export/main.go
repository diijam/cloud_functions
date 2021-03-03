package main

import (
	"log"

	"github.com/diijam/cloud_functions/exporter"
)

func main() {
	if err := exporter.ExportYesterdayFirebaseTableAsJSON(); err != nil {
		log.Printf("Cannot export bigquery table to GCS due to %v", err)
	}
}
