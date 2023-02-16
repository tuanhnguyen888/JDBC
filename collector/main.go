package main

import (
	"collector/database"
	"collector/kafka"
	"context"
	"log"
	"os"
	"strconv"

	"github.com/jasonlvhit/gocron"
	"github.com/joho/godotenv"
)

type Config struct {
	DbName   string
	Dns      string
	Host     string
	Query    string
	Schedule string
}

func initDatabase(config Config) database.Database {

	switch config.DbName {
	case "postgres":
		dataStory := database.NewPostgres(config.Dns, config.Query)
		return dataStory
	case "mysql":
		dataStory := database.NewMysql(config.Dns, config.Query)
		return dataStory
	case "cassandra":
		dataStory := database.NewCassandra(config.Host, config.Query)
		return dataStory
	case "oracle":
		dataStory := database.NewOracle(config.Dns, config.Query)
		return dataStory
	default:
		log.Fatalf("invalid database name")
		return nil
	}
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Panic(err)
	}

	config := &Config{
		DbName:   os.Getenv("DB_NAME"),
		Dns:      os.Getenv("DB_DSN"),
		Host:     os.Getenv("DB_HOST"),
		Query:    os.Getenv("DB_QUERY"),
		Schedule: os.Getenv("DB_SCHEDULE"),
	}

	schedule, err := strconv.Atoi(config.Schedule)
	if err != nil {
		log.Panic(err)
	}

	ctx := context.Background()
	data := initDatabase(*config)
	write := kafka.NewKafkaWriter()

	err = gocron.Every(uint64(schedule)).Second().Do(data.PushLogBySchedule, *write, ctx)
	if err != nil {
		log.Panic(err)
	}

	<-gocron.Start()
}
