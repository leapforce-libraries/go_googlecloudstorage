package googlecloudstorage

import (
	"encoding/json"
	"fmt"
	"reflect"

	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	utilities "github.com/leapforce-libraries/go_utilities"
)

type Logger struct {
	objectHandle *storage.ObjectHandle
	writer       *storage.Writer
	service      *Service
	modelType    reflect.Type
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

	objectHandle := service.bucket.Handle.Object(objectName)
	return &Logger{
		objectHandle: objectHandle,
		writer:       objectHandle.NewWriter(service.context),
		service:      service,
		modelType:    modelType,
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

func (logger *Logger) ToBigQuery(bigQueryService *go_bigquery.Service, sqlConfig *go_bigquery.SqlConfig, truncateTable bool, deleteObject bool) *errortools.Error {
	copyObjectToTableConfig := go_bigquery.CopyObjectToTableConfig{
		ObjectHandle:  logger.objectHandle,
		SqlConfig:     sqlConfig,
		TruncateTable: truncateTable,
		DeleteObject:  deleteObject,
	}

	return bigQueryService.CopyObjectToTable(&copyObjectToTableConfig)
}
