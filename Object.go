package googlecloudstorage

import (
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	"google.golang.org/api/iterator"
)

type Object struct {
	service    *Service
	bucketName string
	Name       string
	Attrs      *storage.ObjectAttrs
	Handle     *storage.ObjectHandle
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
			service.bucket.Name,
			objectAttrs.Name,
			objectAttrs,
			service.bucket.Handle.Object(objectAttrs.Name),
		})
	}

	return &objects, nil
}

func (object *Object) Read(model interface{}) (bool, *errortools.Error) {
	return object.service.readObject(object.Handle, object.service.context, model)
}

func (object *Object) Bytes() (*[]byte, bool, *errortools.Error) {
	return object.service.read(object.Handle, object.service.context)
}

func (object *Object) IsFolder() bool {
	return strings.HasSuffix(object.Name, "/")
}

func (object *Object) InSubFolder() bool {
	return strings.Contains(object.Name, "/")
}

func (object *Object) Delete() *errortools.Error {
	err := object.Handle.Delete(object.service.context)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}

func (object *Object) CopyToSubFolder(folderName string) *errortools.Error {
	objectName := fmt.Sprintf("%s/%s", folderName, object.Name)

	_, err := object.service.bucket.Handle.Object(objectName).CopierFrom(object.Handle).Run(object.service.context)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}

func (object *Object) MoveToSubFolder(folderName string) *errortools.Error {
	e := object.CopyToSubFolder(folderName)
	if e != nil {
		return e
	}

	e = object.Delete()
	if e != nil {
		return e
	}

	return nil
}
