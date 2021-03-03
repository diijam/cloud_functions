package exporter

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/storage"
)

//ErrObjectExisted indicates that object is already existed
var ErrObjectExisted = errors.New("storage: object already existed")

func checkGcsFileNotExisted(ctx context.Context, bucketName, prefix, filename string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(fmt.Sprintf("%s/%s", prefix, filename))
	if _, err := obj.Attrs(ctx); err != nil {
		if err == storage.ErrObjectNotExist {
			return nil
		}
		return err
	}

	return ErrObjectExisted
}
