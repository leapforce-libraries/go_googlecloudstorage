package googlecloudstorage

import (
	"cloud.google.com/go/storage"
	"context"
	errortools "github.com/leapforce-libraries/go_errortools"
	"io"
	"time"
)

type Bucket struct {
	Name string
	//Attrs  *storage.BucketAttrs
	Handle *storage.BucketHandle
}

func (bucket *Bucket) Upload(path string, file io.Reader, timeout *time.Duration) *errortools.Error {
	ctx := context.Background()

	if timeout != nil {
		ctx_, cancel := context.WithTimeout(ctx, *timeout)
		defer cancel()

		ctx = ctx_
	}

	wc := bucket.Handle.Object(path).NewWriter(ctx)
	if _, err := io.Copy(wc, file); err != nil {
		return errortools.ErrorMessagef("io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		return errortools.ErrorMessagef("Writer.Close: %v", err)
	}

	return nil
}
