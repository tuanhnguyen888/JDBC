package database

import (
	"encoding/json"
	"github.com/gocql/gocql"
	"reflect"
	"testing"
)

func TestNewCassandra(t *testing.T) {
	host := "127.0.0.1"
	query := "SELECT * FROM collector.cart"
	db := NewCassandra(host, query)

	if db == nil {
		t.Error("Expected non-nil value, got nil")
	}

	cassandraDb, ok := db.(*cassandraStory)
	if !ok {
		t.Error("Expected cassandraStory type, got", reflect.TypeOf(db))
	}

	if cassandraDb.query != query {
		t.Errorf("Expected query %q, got %q", query, cassandraDb.query)
	}
}

func TestCassandraStory_Execute(t *testing.T) {
	query := "SELECT * FROM collector.cart"
	host := "127.0.0.1"
	cluster := gocql.NewCluster(host)
	cluster.Keyspace = "collector"
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	story := &cassandraStory{
		query: query,
		db:    session,
	}

	result, err := story.Execute()
	if err != nil {
		t.Fatalf("error executing cassandra query: %v", err)
	}

	var logs []map[string]interface{}
	if err := json.Unmarshal(result, &logs); err != nil {
		t.Fatalf("error unmarshaling cassandra result: %v", err)
	}

	// Assert that the result is of the expected type and not empty
	if reflect.TypeOf(logs) != reflect.TypeOf(make([]map[string]interface{}, 0)) {
		t.Errorf("expected type: %v, got type: %v", reflect.TypeOf(make([]map[string]interface{}, 0)), reflect.TypeOf(logs))
	}
	if len(logs) == 0 {
		t.Error("expected result to be non-empty")
	}
}
