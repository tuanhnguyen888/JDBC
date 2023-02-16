package database

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"collector/kafka"
)

type postgresqlStory struct {
	query string
	DB    *gorm.DB
}

func NewPostgres(dsn string, query string) Database {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Panicln(err)
	}
	return &postgresqlStory{
		query: query,
		DB:    db,
	}
}

func (s *postgresqlStory) Execute() ([]byte, error) {
	var logs []map[string]interface{}

	err := s.DB.Raw("SELECT * FROM servers").Scan(&logs).Error
	if err != nil {
		return nil, err
	}

	logsJSON, err := json.Marshal(logs)
	if err != nil {
		return nil, err
	}

	// log.Println(logs)

	return logsJSON, nil
}

func (s *postgresqlStory) PushLogBySchedule(writer kafka.Writer, ctx context.Context) {
	logs, err := s.Execute()
	if err != nil {
		log.Fatal(err)
	}

	err = writer.WriteMessages(ctx, logs)
	if err != nil {
		log.Panic(err)
	}
	log.Println("updated logs", time.Now())
}
