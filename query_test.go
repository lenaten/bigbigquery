package bigbigquery_test

import (
	"context"
	"testing"
	"io"

	"golang.org/x/oauth2/google"
	"github.com/lenaten/bigbigquery"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	dataset := "tmp"
	bucket := "tmp"

	ctx := context.Background()
	credentials, err := google.FindDefaultCredentials(ctx)
	assert.Nil(t, err)
	
	client, err := bigbigquery.NewClient(ctx, credentials.ProjectID, bigbigquery.WithDataset(dataset), bigbigquery.WithBucket(bucket))
	assert.Nil(t, err)

	q := client.Query("SELECT * FROM dataset.table LIMIT 1")
	it, err := q.Read(ctx)
	assert.Nil(t, err)
	
	var i map[string]interface{}
	
	err = it.Next(&i)
	assert.Nil(t, err)

	err = it.Next(&i)
	assert.Equal(t, io.EOF, err)
	
	err = it.Close()
	assert.Nil(t, err)	
}

