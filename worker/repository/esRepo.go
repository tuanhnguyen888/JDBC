package repository

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"log"

	"worker/database"
)

type EsLogRepository interface {
	SaveLog(logEvent []byte) error
}

type esLogRepository struct {
	elasticClent *elastic.Client
}

func NewEsLogRepository() EsLogRepository {
	esClient, err := database.NewInitEsClient()
	if err != nil {
		log.Panicf(err.Error())
	}

	return &esLogRepository{
		elasticClent: esClient.Conn,
	}
}

const mapping = `
{
	"mappings": {
		"log": {
			"properties": {
			  "id": {
				"type": "long"
			  },
			  "level": {
				"type": "keyword"
			  },
			  "provider_name": {
				"type": "keyword"
			  },
			  "msg": {
				"type": "text"
			  },
			  "created_at": {
				"type": "long"
			  },
			  "updated_at": {
				"type": "long"
			  }
			}
		}
	}
}
`

func (r *esLogRepository) SaveLog(logEvent []byte) error {
	ctx := context.Background()
	// Tạo một index với tên "documents"
	exists, err := r.elasticClent.IndexExists("logs").Do(ctx)
	if err != nil {
		// Handle error
		return err
	}
	if !exists {
		// Create a new index.
		createIndex, err := r.elasticClent.CreateIndex("logs").Do(context.Background())
		if err != nil {
			// Xử lý lỗi khi tạo index
			// fmt.Println("Error creating the index: %s", err)
			log.Panicf("Error creating the index: %s", err)
			return errors.New("Error creating the index ")
		}

		if !createIndex.Acknowledged {
			return errors.New("Index not created")
		}
	}

	var doc []map[string]interface{}
	if err := json.Unmarshal(logEvent, &doc); err != nil {
		return errors.New("Failed to unmarshal json data: " + err.Error())
	}

	for _, d := range doc {
		_, err = r.elasticClent.Index().
			Index("logs").
			//Type(indexType).
			BodyJson(d).
			Do(context.Background())
		if err != nil {
			return errors.New("Failed to index document: %v" + err.Error())
		}
	}
	return nil
}

func (r *esLogRepository) ShowLogs() {

}
