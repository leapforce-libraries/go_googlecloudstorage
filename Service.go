package googlecloudstorage

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	go_credentials "github.com/leapforce-libraries/go_google/credentials"
	"google.golang.org/api/option"
)

const defaultTimestampLayout string = "2006-01-02 15:04:05"

type ServiceConfig struct {
	CredentialsJSON *go_credentials.CredentialsJSON
	BucketName      string
	TimestampLayout *string
}

type Service struct {
	credentialsJSON *go_credentials.CredentialsJSON
	bucketHandle    *storage.BucketHandle
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
	clientStorage, err := storage.NewClient(ctx, option.WithCredentialsJSON(credentialsByte))
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	timestampLayout := defaultTimestampLayout
	if serviceConfig.TimestampLayout != nil {
		timestampLayout = *serviceConfig.TimestampLayout
	}

	return &Service{
		credentialsJSON: serviceConfig.CredentialsJSON,
		bucketHandle:    clientStorage.Bucket(serviceConfig.BucketName),
		context:         ctx,
		timestampLayout: timestampLayout,
	}, nil
}
