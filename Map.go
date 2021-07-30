package googlecloudstorage

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
)

const dateLayout string = "2006-01-02"

type Map struct {
	objectHandle *storage.ObjectHandle
	service      *Service
	data         map[string]string
	dirty        bool
}

func (service *Service) NewMap(objectName string, writeOnly bool) (*Map, bool, *errortools.Error) {
	data := make(map[string]string)

	m := Map{
		objectHandle: service.bucket.Handle.Object(objectName),
		service:      service,
		data:         data,
	}

	if writeOnly {
		return &m, true, nil
	}

	exists, e := service.readObject(m.objectHandle, service.context, &data)
	if e != nil {
		return nil, exists, e
	}

	if exists {
		m.data = data
	}

	return &m, exists, nil
}

func (m Map) Keys() []string {
	keys := []string{}

	if m.data != nil {
		for key := range m.data {
			keys = append(keys, key)
		}
	}

	return keys
}

func (m Map) Get(key string) (*string, *errortools.Error) {
	value, ok := m.data[key]

	if !ok {
		return nil, nil
	}

	return &value, nil
}

func (m Map) GetInt64(key string) (*int64, *errortools.Error) {
	value, e := m.Get(key)
	if e != nil {
		return nil, e
	}

	if value == nil {
		return nil, nil
	}

	i, err := strconv.ParseInt(*value, 10, 64)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	return &i, nil
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

func (m Map) GetDate(key string) (*civil.Date, *errortools.Error) {
	value, e := m.Get(key)
	if e != nil {
		return nil, e
	}

	if value == nil {
		return nil, nil
	}

	t, err := time.Parse(dateLayout, *value)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	d := civil.DateOf(t)
	return &d, nil
}

func (m *Map) Set(key string, value string, save bool) *errortools.Error {
	if m == nil {
		return nil
	}

	m.data[key] = value
	m.dirty = true

	if save {
		return m.Save()
	}

	return nil
}

func (m *Map) SetInt64(key string, i int64, save bool) *errortools.Error {
	return m.Set(key, fmt.Sprintf("%v", i), save)
}

func (m *Map) SetTimestamp(key string, value time.Time, save bool) *errortools.Error {
	return m.Set(key, value.Format(m.service.timestampLayout), save)
}

func (m *Map) SetDate(key string, value civil.Date, save bool) *errortools.Error {
	return m.Set(key, value.String(), save)
}

func (m *Map) Delete(key string) {
	if m == nil {
		return
	}

	if m.data == nil {
		return
	}

	delete(m.data, key)
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

	m.dirty = false

	return nil
}
