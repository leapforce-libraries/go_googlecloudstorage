package googlecloudstorage

import (
	"encoding/json"
	"fmt"
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

func (service *Service) NewMap(objectName string, writeOnly bool) (*Map, bool, *errortools.Error) {
	data := make(map[string]string)

	objectHandle := service.bucket.Handle.Object(objectName)

	if !writeOnly {
		exists, e := service.readObject(objectHandle, service.context, &data)
		if e != nil {
			return nil, exists, e
		}
		if !exists {
			return nil, exists, nil
		}
	}

	return &Map{
		objectHandle: objectHandle,
		service:      service,
		data:         data,
	}, true, nil
}

func (m Map) Get(key string) (*string, *errortools.Error) {
	value, ok := m.data[key]

	if !ok {
		return nil, nil
	}

	return &value, nil
}

func (m Map) GetTimestamp(key string) (*time.Time, *errortools.Error) {
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

func (m *Map) Set(key string, value string, save bool) {
	if m == nil {
		return
	}

	m.data[key] = value
	m.dirty = true

	if save {
		m.Save()
	}
}

func (m *Map) SetTimestamp(key string, value time.Time, save bool) {
	m.Set(key, value.Format(m.service.timestampLayout), save)
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

	fmt.Println(string(b))

	// Write data
	if _, err := w.Write(b); err != nil {
		return errortools.ErrorMessage(err)
	}

	// Close
	if err := w.Close(); err != nil {
		return errortools.ErrorMessage(err)
	}

	m.dirty = false

	return nil
}
