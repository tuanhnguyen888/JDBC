package database

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis"
	"log"
	"strconv"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"collector/kafka"
)

type postgresqlStory struct {
	query       string
	dns         string
	DB          *gorm.DB
	redisClient *redis.Client
}

func NewPostgres(dsn string, query string, redisClient *redis.Client) Database {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Panicln(err)
	}
	return &postgresqlStory{
		query:       query,
		DB:          db,
		dns:         dsn,
		redisClient: redisClient,
	}
}

func (s *postgresqlStory) Execute(stt int) ([]byte, error) {
	var logs []map[string]interface{}
	sttStirng := strconv.Itoa(stt)
	// SU dung Redis
	timestamp, err := s.redisClient.Get("timestamp_" + sttStirng).Uint64()

	//dns := s.redisClient.Get("dns_" + sttStirng).String()

	if err != nil {
		err := s.DB.Raw(s.query).Scan(&logs).Error
		if err != nil {
			return nil, err
		}
	} else {
		err = s.DB.Raw(s.query+" where timestamp > ?", timestamp).Scan(&logs).Error
		if err != nil {
			return nil, err
		}
	}

	logsJSON, err := json.Marshal(logs)
	if err != nil {
		return nil, err
	}

	nameTable, err := getNameTableByQuery(s.query)
	if err != nil {
		return nil, err
	}
	err = s.DB.Raw("SELECT MAX(timestamp) FROM " + nameTable).Scan(&timestamp).Error
	if err != nil {
		return nil, err
	}

	err = s.redisClient.Set("timestamp_"+sttStirng, timestamp, 10*time.Hour).Err()
	if err != nil {
		return nil, err
	}

	err = s.redisClient.Set("dns_"+sttStirng, s.dns, 10*time.Hour).Err()
	if err != nil {
		return nil, err
	}

	// log.Println(logs)

	return logsJSON, nil
}

func (s *postgresqlStory) PushLogBySchedule(writer *kafka.Writer, ctx context.Context, stt int) {
	logs, err := s.Execute(stt)
	if err != nil {
		log.Fatal(err)
	}
	if len(string(logs)) != 4 {
		log.Println(len(string(logs)))

		err = writer.WriteMessages(ctx, logs)
		if err != nil {
			log.Panic(err)
		}

		log.Println("updated logs", time.Now())
	}

}
