package googlecloudstorage

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Object struct {
	service *Service
	Name    string
	Attrs   *storage.ObjectAttrs
	Handle  *storage.ObjectHandle
}

func (service *Service) Objects() (*[]*Object, *errortools.Error) {
	objects := []*Object{}

	it := service.bucket.Handle.Objects(service.context, nil)
	for {
		objectAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errortools.ErrorMessage(err)
		}

		objects = append(objects, &Object{
			service,
			objectAttrs.Name,
			objectAttrs,
			service.bucket.Handle.Object(objectAttrs.Name),
		})
	}

	return &objects, nil
}

func (object *Object) Read(model interface{}) *errortools.Error {
	return object.service.readObject(object.Handle, object.service.context, model)
}

func (object *Object) CopyToFolder(folderName string) *errortools.Error {
	bucketName := fmt.Sprintf("%s/%s", object.service.bucket.Attrs.Name, folderName)

	credentialsByte, err := json.Marshal(object.service.credentialsJSON)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	clientStorage, err := storage.NewClient(object.service.context, option.WithCredentialsJSON(credentialsByte))
	if err != nil {
		return errortools.ErrorMessage(err)
	}
	_, err = clientStorage.Bucket(bucketName).Object(object.Attrs.Name).CopierFrom(object.Handle).Run(object.service.context)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}
