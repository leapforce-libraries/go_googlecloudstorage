package googlecloudstorage

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"io/ioutil"

	errortools "github.com/leapforce-libraries/go_errortools"
	go_credentials "github.com/leapforce-libraries/go_google/credentials"
	"google.golang.org/api/option"
)

const (
	defaultTimestampLayout string = "2006-01-02 15:04:05"
	BaseUrl                string = "https://storage.googleapis.com"
)

type ServiceConfig struct {
	CredentialsJson   *go_credentials.CredentialsJson
	DefaultBucketName *string
	TimestampLayout   *string
}

type Service struct {
	storageClient   *storage.Client
	bucket          *Bucket
	context         context.Context
	timestampLayout string
}

func NewService(serviceConfig *ServiceConfig) (*Service, *errortools.Error) {
	if serviceConfig.CredentialsJson == nil {
		return nil, errortools.ErrorMessage("CredentialsJSON not provided")
	}

	credentialsByte, err := json.Marshal(&serviceConfig.CredentialsJson)
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

func (service *Service) read(objectHandle *storage.ObjectHandle, ctx context.Context) (*[]byte, bool, *errortools.Error) {
	reader, err := objectHandle.NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		//fmt.Println("file does not exist")
		return nil, false, nil
	}
	if err != nil {
		return nil, true, errortools.ErrorMessage(err)
	}
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, true, errortools.ErrorMessage(err)
	}

	return &b, true, nil
}

func (service *Service) readObject(objectHandle *storage.ObjectHandle, ctx context.Context, model interface{}) (bool, *errortools.Error) {
	b, exists, e := service.read(objectHandle, ctx)
	if e != nil {
		return exists, e
	}
	if !exists {
		return exists, nil
	}

	err := json.Unmarshal(*b, model)
	if err != nil {
		return true, errortools.ErrorMessage(err)
	}

	return true, nil
}
