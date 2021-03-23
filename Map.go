package googlecloudstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
)

type Map struct {
	objectHandle *storage.ObjectHandle
	context      context.Context
	data         map[string]string
}

func (service *Service) NewMap(objectName string) (*Map, *errortools.Error) {
	data := make(map[string]string)

	objAppMem := service.bucketHandle.Object(objectName)
	reader, err := objAppMem.NewReader(service.context)
	if err == storage.ErrObjectNotExist {
		// file does not exist
		fmt.Println("file does not exist")
	} else if err != nil {
		return nil, errortools.ErrorMessage(err)
	} else {
		b, err := ioutil.ReadAll(reader)
		if err != nil {
			return nil, errortools.ErrorMessage(err)
		}
		err = json.Unmarshal(b, &data)
		if err != nil {
			return nil, errortools.ErrorMessage(err)
		}
	}

	return &Map{
		objectHandle: objAppMem,
		context:      service.context,
		data:         data,
	}, nil
}

func (m *Map) Get(key string) (*string, *errortools.Error) {
	if m == nil {
		return nil, errortools.ErrorMessage("Map is a nil pointer")
	}

	value, ok := m.data[key]

	if !ok {
		return nil, nil
	}

	return &value, nil
}

func (m *Map) Set(key string, value string) *errortools.Error {
	if m == nil {
		return errortools.ErrorMessage("Map is a nil pointer")
	}

	m.data[key] = value

	w := m.objectHandle.NewWriter(m.context)
	b, err := json.Marshal(m.data)
	if err != nil {
		errortools.CaptureFatal(err)
	}

	// Write data
	if _, err := w.Write(b); err != nil {
		return errortools.ErrorMessage(err)
	}

	// Close
	if err := w.Close(); err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}
