package database

import (
	"fmt"

	"github.com/olivere/elastic/v7"
)

type elasticConn struct {
	Conn *elastic.Client
}

var es *elasticConn

func NewInitEsClient() (*elasticConn, error) {
	if es == nil {
		client, err := elastic.NewClient(elastic.SetURL("http://localhost:9200/"),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(false))

		if err != nil {
			return nil, err
		}

		fmt.Println("ES initialized...")

		es = &elasticConn{Conn: client}
	}

	return es, nil

}
