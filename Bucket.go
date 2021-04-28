package googlecloudstorage

import (
	"cloud.google.com/go/storage"
)

type Bucket struct {
	Name string
	//Attrs  *storage.BucketAttrs
	Handle *storage.BucketHandle
}
