package database

import (
	"collector/kafka"
	"context"
	"database/sql"
	"encoding/json"
	//_ "gopkg.in/goracle.v2"
	"log"
	"time"
)

type oracleStory struct {
	query string
	DB    *sql.DB
}

func NewOracle(dsn string, query string) Database {
	db, err := sql.Open("oci8", dsn)
	if err != nil {
		log.Panicln(err)
	}

	return &oracleStory{
		query: query,
		DB:    db,
	}
}

func (s *oracleStory) Execute() ([]byte, error) {
	var logs []map[string]interface{}

	rows, err := s.DB.QueryContext(context.Background(), "SELECT * FROM employees")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// data column
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(sql.RawBytes)
		}

		err = rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		// tao map du lieu
		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i].(sql.RawBytes)
			row[col] = string(val)
		}

		logs = append(logs, row)
	}

	log.Println(logs)

	logsJSON, err := json.Marshal(logs)
	if err != nil {
		return nil, err
	}

	// log.Println(logs)

	return logsJSON, nil
}

func (s *oracleStory) PushLogBySchedule(writer kafka.Writer, ctx context.Context) {
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
