package bigbigquery

import (
	"encoding/json"
	"os"

	"cloud.google.com/go/bigquery"
)

// A RowIterator provides access to the result of a BigQuery lookup.
type RowIterator struct {
	// The schema of the table.
	Schema bigquery.Schema
	// The total number of rows in the result.
	TotalRows uint64

	decoder  *json.Decoder
	filepath string
}

func (it *RowIterator) Next(dst interface{}) error {
	return it.decoder.Decode(&dst)
}

func (it RowIterator) Close() error {
	return os.Remove(it.filepath)
}
