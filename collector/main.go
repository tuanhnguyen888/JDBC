package main

import (
	"collector/cache"
	"collector/database"
	"collector/kafka"
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jasonlvhit/gocron"

	"github.com/joho/godotenv"
)

type dbConfig struct {
	DbName   string
	Dsn      string
	Host     string
	Query    string
	Schedule int
}

func initDatabase(config dbConfig, redisClient *redis.Client) database.Database {

	switch config.DbName {
	case "postgres":
		dataStory := database.NewPostgres(config.Dsn, config.Query, redisClient)
		return dataStory
	case "mysql":
		dataStory := database.NewMysql(config.Dsn, config.Query)
		return dataStory
	case "cassandra":
		dataStory := database.NewCassandra(config.Host, config.Query)
		return dataStory
	case "oracle":
		dataStory := database.NewOracle(config.Dsn, config.Query)
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

	// Get the number of databases
	numDbs := 0
	for {
		_, ok := os.LookupEnv(fmt.Sprintf("DB_NAME_%d", numDbs))
		if !ok {
			break
		}
		numDbs++
	}

	// Create a slice to hold the database configs
	dbConfigs := make([]dbConfig, numDbs)

	// Loop through each database config and add it to the slice
	for i := 0; i < numDbs; i++ {
		name, _ := os.LookupEnv(fmt.Sprintf("DB_NAME_%d", i))
		host, _ := os.LookupEnv(fmt.Sprintf("DB_HOST_%d", i))
		dsn, _ := os.LookupEnv(fmt.Sprintf("DB_DSN_%d", i))
		query, _ := os.LookupEnv(fmt.Sprintf("DB_QUERY_%d", i))
		schedule, _ := os.LookupEnv(fmt.Sprintf("DB_SCHEDULE_%d", i))
		scheduleInt, err := strconv.Atoi(schedule)
		if err != nil {
			fmt.Printf("Error parsing DB_SCHEDULE_%d: %v\n", i, err)
			os.Exit(1)
		}

		dbConfigs[i] = dbConfig{
			DbName:   name,
			Host:     host,
			Dsn:      dsn,
			Query:    query,
			Schedule: scheduleInt,
		}
	}

	redisConn, err := cache.NewInitRedis()
	if err != nil {
		log.Panic(err)
	}

	writer := kafka.NewKafkaWriter()
	ctx := context.Background()
	for i, config := range dbConfigs {
		go func(config dbConfig) {
			data := initDatabase(config, redisConn.Conn)

			err = gocron.Every(uint64(config.Schedule)).Second().Do(data.PushLogBySchedule, writer, ctx, i)
			if err != nil {
				log.Panic(err)
			}

			<-gocron.Start()
		}(config)
	}

	for {
		time.Sleep(time.Hour)
	}
}
