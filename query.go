package bigbigquery

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

type Query struct {
	client      *Client
	QueryConfig bigquery.QueryConfig
}

func (c *Client) Query(q string) *Query {
	return &Query{
		client:      c,
		QueryConfig: bigquery.QueryConfig{Q: q},
	}
}

func (q *Query) Read(ctx context.Context) (*RowIterator, error) {
	qid := strings.Replace(uuid.New().String(), "-", "_", -1)

	// create table from query
	qq := q.client.bigqueryClient.Query(q.QueryConfig.Q)
	qq.CreateDisposition = bigquery.CreateIfNeeded
	qq.WriteDisposition = bigquery.WriteTruncate
	qq.Dst = q.client.bigqueryClient.Dataset(q.client.dataset).Table(qid)
	queryJob, err := qq.Run(ctx)
	if err != nil {
		return nil, err
	}
	queryStatus, err := queryJob.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if err := queryStatus.Err(); err != nil {
		return nil, err
	}
	md, err := q.client.bigqueryClient.Dataset(q.client.dataset).Table(qid).Metadata(ctx)
	if err != nil {
		return nil, err
	}

	// copy table to gs
	gcsURI := fmt.Sprintf("gs://%s/%s/*.json.gz", q.client.bucket, qid)
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.Compression = bigquery.Gzip
	gcsRef.DestinationFormat = bigquery.JSON
	extractor := q.client.bigqueryClient.Dataset(q.client.dataset).Table(qid).ExtractorTo(gcsRef)
	extractJob, err := extractor.Run(ctx)
	if err != nil {
		return nil, err
	}
	extractStatus, err := extractJob.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if err := extractStatus.Err(); err != nil {
		return nil, err
	}

	// copy from gs to fs
	filepath := path.Join(os.TempDir(), qid+".json.gz")
	file, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}
	gzipWriter := gzip.NewWriter(file)
	it := q.client.storageClient.Bucket(q.client.bucket).Objects(ctx, &storage.Query{Prefix: qid})
	for {
		object, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		objectReader, err := q.client.storageClient.Bucket(q.client.bucket).Object(object.Name).NewReader(ctx)
		if err != nil {
			return nil, err
		}
		gzipReader, err := gzip.NewReader(objectReader)
		if err != nil {
			return nil, err
		}
		if _, err = io.Copy(gzipWriter, gzipReader); err != nil {
			return nil, err
		}
		if err = gzipReader.Close(); err != nil {
			return nil, err
		}
		if err = gzipWriter.Flush(); err != nil {
			return nil, err
		}
		if err = gzipWriter.Close(); err != nil {
			return nil, err
		}
	}
	if err = file.Close(); err != nil {
		return nil, err
	}

	// clean bigquery
	if err = q.client.bigqueryClient.Dataset(q.client.dataset).Table(qid).Delete(ctx); err != nil {
		return nil, err
	}

	// clean storage
	it = q.client.storageClient.Bucket(q.client.bucket).Objects(ctx, &storage.Query{Prefix: qid})
	for {
		object, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		if err = q.client.storageClient.Bucket(q.client.bucket).Object(object.Name).Delete(ctx); err != nil {
			return nil, err
		}
	}

	file, err = os.Open(filepath)
	if err != nil {
		return nil, err
	}
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(gzipReader)

	return &RowIterator{
		Schema:    md.Schema,
		TotalRows: md.NumRows,
		decoder:   decoder,
		filepath:  filepath,
	}, nil
}
