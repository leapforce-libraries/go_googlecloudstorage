package googlecloudstorage

import (
	"encoding/json"

	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
)

type Value struct {
	objectHandle *storage.ObjectHandle
	service      *Service
	bytes        []byte
	dirty        bool
}

func (service *Service) NewValue(objectName string, writeOnly bool) (*Value, *errortools.Error) {
	var bytes []byte

	objAppMem := service.bucket.Handle.Object(objectName)

	if !writeOnly {
		b, e := service.read(objAppMem, service.context)
		if e != nil {
			return nil, e
		}

		bytes = *b
	}

	return &Value{
		objectHandle: objAppMem,
		service:      service,
		bytes:        bytes,
	}, nil
}

func (v *Value) GetString() (*string, *errortools.Error) {
	if v == nil {
		return nil, errortools.ErrorMessage("Value is a nil pointer")
	}

	s := string(v.bytes)

	return &s, nil
}

func (v *Value) GetStruct(model interface{}) *errortools.Error {
	if v == nil {
		return errortools.ErrorMessage("Value is a nil pointer")
	}

	err := json.Unmarshal(v.bytes, model)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}

func (v *Value) SetString(s string, save bool) *errortools.Error {
	if v == nil {
		return errortools.ErrorMessage("Value is a nil pointer")
	}

	v.bytes = []byte(s)
	v.dirty = true

	if save {
		return v.Save()
	}

	return nil
}

func (v *Value) SetStruct(value interface{}, save bool) *errortools.Error {
	if v == nil {
		return errortools.ErrorMessage("Value is a nil pointer")
	}

	b, err := json.Marshal(value)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	v.bytes = b
	v.dirty = true

	if save {
		return v.Save()
	}

	return nil
}

func (v *Value) Save() *errortools.Error {
	if !v.dirty {
		return nil
	}

	w := v.objectHandle.NewWriter(v.service.context)
	/*b, err := json.Marshal(v.value)
	if err != nil {
		errortools.CaptureFatal(err)
	}*/

	// Write data
	if _, err := w.Write(v.bytes); err != nil {
		return errortools.ErrorMessage(err)
	}

	// Close
	if err := w.Close(); err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}
