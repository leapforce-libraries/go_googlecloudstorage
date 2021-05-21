package googlecloudstorage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
)

type Value struct {
	objectHandle *storage.ObjectHandle
	service      *Service
	bytes        []byte
	dirty        bool
}

func (service *Service) NewValue(objectName string, writeOnly bool) (*Value, bool, *errortools.Error) {
	value := Value{
		objectHandle: service.bucket.Handle.Object(objectName),
		service:      service,
		bytes:        []byte{},
	}

	if writeOnly {
		return &value, true, nil
	}

	b, exists, e := service.read(value.objectHandle, service.context)
	if e != nil {
		return nil, exists, e
	}

	if exists {
		value.bytes = *b
	}

	return &value, exists, nil
}

func (v Value) GetString() *string {
	if len(v.bytes) == 0 {
		return nil
	}

	s := string(v.bytes)

	return &s
}

func (v Value) GetTimestamp() (*time.Time, *errortools.Error) {
	s := v.GetString()

	if s == nil {
		return nil, nil
	}

	t, err := time.Parse(v.service.timestampLayout, *s)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	return &t, nil
}

func (v Value) GetStruct(model interface{}) *errortools.Error {
	if len(v.bytes) == 0 {
		return nil
	}

	err := json.Unmarshal(v.bytes, model)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}

func (v *Value) SetString(s string, save bool) *errortools.Error {
	if v == nil {
		return nil
	}

	v.bytes = []byte(s)
	v.dirty = true

	if save {
		return v.Save()
	}

	return nil
}

func (v *Value) SetTimestamp(t time.Time, save bool) *errortools.Error {
	return v.SetString(t.Format(v.service.timestampLayout), save)
}

func (v *Value) AddString(s string, separator string, distinct bool, save bool) *errortools.Error {
	if v == nil {
		return nil
	}

	st := v.GetString()
	if st == nil {
		return nil
	}

	if distinct { // store only distinct strings
		if strings.Contains(fmt.Sprintf("%s%s%s", separator, *st, separator), fmt.Sprintf("%s%s%s", separator, s, separator)) {
			return nil
		}
	}
	_st := strings.Split(*st, separator)
	_st = append(_st, s)

	return v.SetString(strings.Join(_st, separator), save)
}

func (v *Value) SetStruct(value interface{}, save bool) *errortools.Error {
	if v == nil {
		return nil
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

	v.dirty = false

	return nil
}
