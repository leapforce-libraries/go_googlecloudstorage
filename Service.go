package googlecloudstorage

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	go_credentials "github.com/leapforce-libraries/go_google/credentials"
	"google.golang.org/api/option"
)

type ServiceConfig struct {
	CredentialsJSON *go_credentials.CredentialsJSON
	BucketName      string
}

type Service struct {
	credentialsJSON *go_credentials.CredentialsJSON
	bucketHandle    *storage.BucketHandle
	context         context.Context
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

	return &Service{
		credentialsJSON: serviceConfig.CredentialsJSON,
		bucketHandle:    clientStorage.Bucket(serviceConfig.BucketName),
		context:         ctx,
	}, nil
}
