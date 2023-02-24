package database

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"collector/kafka"
)

type mysqlStory struct {
	query string
	DB    *gorm.DB
}

func NewMysql(dsn string, query string) Database {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Panicln(err)
	}
	return &mysqlStory{
		query: query,
		DB:    db,
	}
}

func (s *mysqlStory) Execute(stt int) ([]byte, error) {
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

func (s *mysqlStory) PushLogBySchedule(writer *kafka.Writer, ctx context.Context, stt int) {
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
