package repository

import (
	"context"
	"github.com/olivere/elastic/v7"
	"testing"
)

func TestSaveLog(t *testing.T) {
	// create a new elasticsearch client
	client, err := elastic.NewClient(
		elastic.SetURL("http://localhost:9200"),
		elastic.SetSniff(false),
	)
	if err != nil {
		t.Fatalf("Failed to create elasticsearch client: %v", err)
	}

	// create a new mock context
	ctx := context.Background()

	// create a new repository with the elasticsearch client
	repo := &esLogRepository{elasticClent: client}

	// create a test document
	doc := []byte(`
		{
			"message": "test message",
			"timestamp": "2022-03-08T08:00:00Z"
		}
	`)

	// test creating a new index if it doesn't exist
	err = repo.SaveLog(doc)
	if err != nil {
		t.Fatalf("Failed to save log: %v", err)
	}

	// assert that the index now exists
	exists, err := client.IndexExists("logs").Do(ctx)
	if err != nil {
		t.Fatalf("Failed to check index existence: %v", err)
	}
	if !exists {
		t.Error("Expected index to exist, but it doesn't")
	}

	// test adding a document to the index
	err = repo.SaveLog(doc)
	if err != nil {
		t.Fatalf("Failed to save log: %v", err)
	}

	// assert that the document was added to the index
	searchResult, err := client.Search().
		Index("logs").
		Query(elastic.NewMatchQuery("message", "test message")).
		Do(ctx)
	if err != nil {
		t.Fatalf("Failed to search logs: %v", err)
	}
	if searchResult.Hits.TotalHits.Value != 1 {
		t.Errorf("Expected 1 log in the index, but found %d", searchResult.Hits.TotalHits.Value)
	}
}
