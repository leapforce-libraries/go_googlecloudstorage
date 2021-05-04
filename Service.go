package googlecloudstorage

import (
	"context"
	"encoding/json"
	"io/ioutil"

	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	go_credentials "github.com/leapforce-libraries/go_google/credentials"
	"google.golang.org/api/option"
)

const defaultTimestampLayout string = "2006-01-02 15:04:05"

type ServiceConfig struct {
	CredentialsJSON   *go_credentials.CredentialsJSON
	DefaultBucketName *string
	TimestampLayout   *string
}

type Service struct {
	//credentialsJSON *go_credentials.CredentialsJSON
	storageClient   *storage.Client
	bucket          *Bucket
	context         context.Context
	timestampLayout string
}

func NewService(serviceConfig *ServiceConfig) (*Service, *errortools.Error) {
	if serviceConfig.CredentialsJSON == nil {
		return nil, errortools.ErrorMessage("CredentialsJSON not provided")
	}

	credentialsByte, err := json.Marshal(&serviceConfig.CredentialsJSON)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	ctx := context.Background()

	// init Google Cloud Storage client
	storageClient, err := storage.NewClient(ctx, option.WithCredentialsJSON(credentialsByte))
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	timestampLayout := defaultTimestampLayout
	if serviceConfig.TimestampLayout != nil {
		timestampLayout = *serviceConfig.TimestampLayout
	}

	service := Service{
		storageClient:   storageClient,
		bucket:          nil,
		context:         ctx,
		timestampLayout: timestampLayout,
	}

	if serviceConfig.DefaultBucketName != nil {
		service.bucket = service.Bucket(*serviceConfig.DefaultBucketName)
	}

	return &service, nil
}

func (service *Service) Bucket(bucketName string) *Bucket {
	bucketHandle := service.storageClient.Bucket(bucketName)
	return &Bucket{
		bucketName,
		bucketHandle,
	}
}

func (service *Service) read(objectHandle *storage.ObjectHandle, ctx context.Context) (*[]byte, *errortools.Error) {
	reader, err := objectHandle.NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		//fmt.Println("file does not exist")
		return nil, errortools.ErrorMessage("File does not exist")
	}
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	return &b, nil
}

func (service *Service) readObject(objectHandle *storage.ObjectHandle, ctx context.Context, model interface{}) *errortools.Error {
	b, e := service.read(objectHandle, ctx)
	if e != nil {
		return e
	}

	err := json.Unmarshal(*b, model)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}
