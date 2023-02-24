package database

import (
	"collector/kafka"
	"context"
	"github.com/pkg/errors"
	"regexp"
)

type Database interface {
	Execute(stt int) ([]byte, error)
	PushLogBySchedule(writer *kafka.Writer, ctx context.Context, stt int)
}

func getNameTableByQuery(query string) (string, error) {

	// Biểu thức chính quy để tìm kiếm tên bảng
	pattern := regexp.MustCompile("FROM\\s+(\\S+)")

	// Tìm kiếm và trích xuất tên bảng từ chuỗi truy vấn
	match := pattern.FindStringSubmatch(query)
	if len(match) > 1 {
		return match[1], nil
	} else {
		return "", errors.New("Table name not found")
	}
}
