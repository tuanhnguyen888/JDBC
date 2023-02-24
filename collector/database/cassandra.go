package database

import (
	"collector/kafka"
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/gocql/gocql"
)

type cassandraStory struct {
	query string
	db    *gocql.Session
}

func NewCassandra(host string, query string) Database {
	cluster := gocql.NewCluster(host)
	cluster.Keyspace = "collector"
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		log.Panicln(err)
	}

	return &cassandraStory{
		query: query,
		db:    session,
	}
}

func (s *cassandraStory) Execute(stt int) ([]byte, error) {
	var logs []map[string]interface{}

	logs, err := s.db.Query(s.query).Iter().SliceMap()
	if err != nil {
		return nil, err
	}

	logsJSON, err := json.Marshal(logs)
	if err != nil {
		return nil, err
	}
	return logsJSON, nil
}

func (s *cassandraStory) PushLogBySchedule(writer *kafka.Writer, ctx context.Context, stt int) {
	logs, err := s.Execute(stt)
	if err != nil {
		log.Panic(err)
	}

	err = writer.WriteMessages(ctx, logs)
	if err != nil {
		log.Panic(err)
	}

	log.Println("updated logs", time.Now())
}
