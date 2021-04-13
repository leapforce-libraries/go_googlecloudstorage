package googlecloudstorage

import (
	"encoding/json"
	"fmt"
	"reflect"

	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	utilities "github.com/leapforce-libraries/go_utilities"
)

type Logger struct {
	writer       *storage.Writer
	service      *Service
	modelType    reflect.Type
	ObjectHandle *storage.ObjectHandle
}

func (service *Service) NewLogger(objectName string, schema interface{}) (*Logger, *errortools.Error) {
	if utilities.IsNil(schema) {
		return nil, errortools.ErrorMessage("Schema must be a pointer to a struct")
	}

	modelType := reflect.TypeOf(schema)

	if modelType.Kind() != reflect.Ptr {
		return nil, errortools.ErrorMessage("Schema must be a pointer to a struct")
	}

	if modelType.Elem().Kind() != reflect.Struct {
		return nil, errortools.ErrorMessage("Schema must be a pointer to a struct")
	}

	objectHandle := service.bucketHandle.Object(objectName)
	return &Logger{
		writer:       objectHandle.NewWriter(service.context),
		service:      service,
		modelType:    modelType,
		ObjectHandle: objectHandle,
	}, nil
}

func (logger *Logger) Write(data interface{}) *errortools.Error {
	if logger == nil {
		return errortools.ErrorMessage("Logger is a nil pointer")
	}

	if reflect.TypeOf(data) != logger.modelType {
		return errortools.ErrorMessage("Invalid type of data")
	}

	b, err := json.Marshal(data)
	if err != nil {
		errortools.CaptureFatal(err)
	}

	// Write data
	if _, err := logger.writer.Write(b); err != nil {
		return errortools.ErrorMessage(err)
	}

	// Write NewLine
	_, err = fmt.Fprintf(logger.writer, "\n")
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}

func (logger *Logger) Close() *errortools.Error {
	err := logger.writer.Close()
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}