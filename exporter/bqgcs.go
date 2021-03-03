package exporter

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
)

// ExportTableAsJSON Export Firebase event table of targetDate to given gcs bucket
func ExportTableAsJSON(projectId, bucketName, prefix string, targetDate time.Time) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	srcProject := projectId
	srcDataset := "analytics_232948146"

	formattedDate := targetDate.Format("20060102")
	srcTable := fmt.Sprintf("events_%s", formattedDate)
	outputFilename := fmt.Sprintf("%s.json.gz", formattedDate)
	gcsURI := fmt.Sprintf("gs://%s/%s/%s", bucketName, prefix, outputFilename)

	if err := checkGcsFileNotExisted(ctx, bucketName, prefix, outputFilename); err != nil {
		log.Print("Target file already existed")
		return err
	}

	log.Printf("Exporting table %s to gcs file %s\n", srcTable, gcsURI)

	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.DestinationFormat = bigquery.JSON
	gcsRef.Compression = bigquery.Gzip

	extractor := client.DatasetInProject(srcProject, srcDataset).Table(srcTable).ExtractorTo(gcsRef)

	job, err := extractor.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	log.Print("Finished")
	return nil
}

// ExportYesterdayFirebaseTableAsJSON Export yesterday firebase table as JSON to GCS
func ExportYesterdayFirebaseTableAsJSON() error {
	yesterday := time.Now().AddDate(0, 0, -1)
	if err := ExportTableAsJSON("diijam", "diijam-app-analytics-events", "raw", yesterday); err != nil {
		log.Fatalf("Cannot export table to GCS due to %v", err)
		return err
	}
	return nil
}
