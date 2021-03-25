package googlecloudstorage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
)

type Map struct {
	objectHandle *storage.ObjectHandle
	service      *Service
	data         map[string]string
	dirty        bool
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
		service:      service,
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

func (m *Map) GetTimestamp(key string) (*time.Time, *errortools.Error) {
	value, e := m.Get(key)
	if e != nil {
		return nil, e
	}

	if value == nil {
		return nil, nil
	}

	t, err := time.Parse(m.service.timestampLayout, *value)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	return &t, nil
}

func (m *Map) Set(key string, value string, save bool) *errortools.Error {
	if m == nil {
		return errortools.ErrorMessage("Map is a nil pointer")
	}

	m.data[key] = value

	if save {
		return m.Save()
	}

	m.dirty = true

	return nil
}

func (m *Map) SetTimestamp(key string, value time.Time, save bool) *errortools.Error {
	return m.Set(key, value.Format(m.service.timestampLayout), save)
}

func (m *Map) Save() *errortools.Error {
	if !m.dirty {
		return nil
	}

	w := m.objectHandle.NewWriter(m.service.context)
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
