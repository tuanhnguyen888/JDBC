package database_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"worker/database"
)

func TestNewInitEsClient(t *testing.T) {
	// Test when Elasticsearch is not initialized
	es, err := database.NewInitEsClient()
	assert.NotNil(t, es.Conn)
	assert.NoError(t, err)

	// Test when Elasticsearch is already initialized
	es2, err2 := database.NewInitEsClient()
	assert.Equal(t, es, es2)
	assert.NoError(t, err2)
}
