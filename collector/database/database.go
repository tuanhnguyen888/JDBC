package database

import (
	"collector/kafka"
	"context"
)

type Database interface {
	Execute() ([]byte, error)
	PushLogBySchedule(writer kafka.Writer, ctx context.Context)
}
