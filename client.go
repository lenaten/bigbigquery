package bigbigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

type Option func(*Client)

func WithDataset(dataset string) Option {
	return func(c *Client) {
		c.dataset = dataset
	}
}

func WithBucket(bucket string) Option {
	return func(c *Client) {
		c.bucket = bucket
	}
}

// Client may be used to perform BigQuery operations.
type Client struct {
	ctx            context.Context
	bigqueryClient *bigquery.Client
	storageClient  *storage.Client
	dataset        string
	bucket         string
}

// NewClient constructs a new Client which can perform BigQuery operations.
// Operations performed via the client are billed to the specified GCP project.
func NewClient(ctx context.Context, projectID string, opts ...Option) (*Client, error) {
	bigqueryClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	c := Client{
		ctx:            ctx,
		bigqueryClient: bigqueryClient,
		storageClient:  storageClient,
	}

	for _, o := range opts {
		o(&c)
	}

	return &c, nil
}
