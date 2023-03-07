package googlecloudstorage

import (
	"encoding/json"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
)

const dateLayout string = "2006-01-02"

type Map struct {
	sync.RWMutex
	objectHandle *storage.ObjectHandle
	service      *Service
	isVirtual    bool
	data         map[string]json.RawMessage
	dirty        bool
}

func (service *Service) NewMap(objectName string, writeOnly bool) (*Map, *errortools.Error) {
	m := Map{
		objectHandle: service.bucket.Handle.Object(objectName),
		service:      service,
	}

	if writeOnly {
		m.data = make(map[string]json.RawMessage)
		return &m, nil
	}

	e := readMap(&m)
	if e != nil {
		return nil, e
	}

	return &m, nil
}

func (service *Service) RefreshMap(m *Map) *errortools.Error {
	return readMap(m)
}

func readMap(m *Map) *errortools.Error {
	m.Lock()
	defer m.Unlock()

	data := make(map[string]json.RawMessage)

	exists, e := m.service.readObject(m.objectHandle, m.service.context, &data)
	if e != nil {
		return e
	}

	m.data = data
	m.isVirtual = !exists

	return nil
}

func (m *Map) IsVirtual() bool {
	return m.isVirtual
}

func (m *Map) Keys() []string {
	m.Lock()
	defer m.Unlock()

	var keys []string

	if m.data != nil {
		for key := range m.data {
			keys = append(keys, key)
		}
	}

	return keys
}

func (m *Map) Get(key string) (*string, *errortools.Error) {
	m.Lock()
	defer m.Unlock()

	value, ok := m.data[key]
	if !ok {
		return nil, nil
	}

	s := ""
	err := json.Unmarshal(value, &s)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	return &s, nil
}

func (m *Map) GetInt64(key string) (*int64, *errortools.Error) {
	m.Lock()
	defer m.Unlock()

	value, ok := m.data[key]
	if !ok {
		return nil, nil
	}

	i := int64(0)
	err := json.Unmarshal(value, &i)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	return &i, nil
}

func (m *Map) GetTimestamp(key string) (*time.Time, *errortools.Error) {
	s, e := m.Get(key) // locking is here
	if e != nil {
		return nil, e
	}

	if s == nil {
		return nil, nil
	}

	t, err := time.Parse(m.service.timestampLayout, *s)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	return &t, nil
}

func (m *Map) GetDate(key string) (*civil.Date, *errortools.Error) {
	s, e := m.Get(key) // locking is here
	if e != nil {
		return nil, e
	}

	if s == nil {
		return nil, nil
	}

	t, err := time.Parse(dateLayout, *s)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	d := civil.DateOf(t)
	return &d, nil
}

func (m *Map) GetObject(key string, model interface{}) (bool, *errortools.Error) {
	m.Lock()
	defer m.Unlock()

	value, ok := m.data[key]
	if !ok {
		return false, nil
	}

	err := json.Unmarshal(value, model)
	if err != nil {
		return false, errortools.ErrorMessage(err)
	}

	return true, nil
}

func (m *Map) set(key string, value interface{}, save bool) *errortools.Error {
	if m == nil {
		return nil
	}

	m.Lock()
	defer m.Unlock()

	b, err := json.Marshal(value)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	m.data[key] = b
	m.dirty = true

	if save {
		return m.save()
	}

	return nil
}

func (m *Map) Set(key string, s string, save bool) *errortools.Error {
	return m.set(key, s, save)
}

func (m *Map) SetInt64(key string, i int64, save bool) *errortools.Error {
	return m.set(key, i, save)
}

func (m *Map) SetTimestamp(key string, value time.Time, save bool) *errortools.Error {
	return m.set(key, value.Format(m.service.timestampLayout), save)
}

func (m *Map) SetDate(key string, value civil.Date, save bool) *errortools.Error {
	return m.set(key, value.String(), save)
}

func (m *Map) SetObject(key string, object interface{}, save bool) *errortools.Error {
	return m.set(key, object, save)
}

func (m *Map) Delete(key string) {
	if m == nil {
		return
	}

	if m.data == nil {
		return
	}

	m.Lock()
	defer m.Unlock()

	delete(m.data, key)
}

func (m *Map) Save() *errortools.Error {
	m.Lock()
	defer m.Unlock()

	return m.save()
}

func (m *Map) save() *errortools.Error {
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
