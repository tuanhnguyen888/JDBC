package database

import (
	"context"
	"encoding/json"
	"gorm.io/gorm"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"collector/kafka"
)

// Tạo mock DB cho mysqlStory
type mockDB struct {
	mock.Mock
}

func (m *mockDB) Raw(sql string, args ...interface{}) *gorm.DB {
	return m.Called(sql, args).Get(0).(*gorm.DB)
}

func (m *mockDB) Scan(dest interface{}) error {
	return m.Called(dest).Error(0)
}

func TestExecute(t *testing.T) {
	// Tạo mock DB và đặt giá trị trả về cho Scan()
	db := new(mockDB)
	expectedLogs := []map[string]interface{}{
		{
			"id":   1,
			"name": "server1",
		},
		{
			"id":   2,
			"name": "server2",
		},
	}
	db.On("Scan", mock.AnythingOfType("*[]map[string]interface {}")).Return(nil).Run(func(args mock.Arguments) {
		dest := args.Get(0).(*[]map[string]interface{})
		*dest = expectedLogs
	})

	// Tạo mysqlStory với DB là mock DB
	mysqlStory := &mysqlStory{
		DB: db,
	}

	// Gọi hàm Execute() và so sánh kết quả trả về với giá trị mong đợi
	expectedJSON, _ := json.Marshal(expectedLogs)
	actualJSON, err := mysqlStory.Execute()
	assert.Nil(t, err)
	assert.Equal(t, expectedJSON, actualJSON)

	// Kiểm tra các mock DB đã được gọi đúng cách
	db.AssertCalled(t, "Scan", mock.AnythingOfType("*[]map[string]interface {}"))
}

func TestPushLogBySchedule(t *testing.T) {
	// Tạo mock Kafka writer
	writer := kafka.NewMockWriter()

	// Tạo mock DB và đặt giá trị trả về cho Execute()
	db := new(mockDB)
	expectedLogs := []map[string]interface{}{
		{
			"id":   1,
			"name": "server1",
		},
		{
			"id":   2,
			"name": "server2",
		},
	}
	db.On("Scan", mock.AnythingOfType("*[]map[string]interface {}")).Return(nil).Run(func(args mock.Arguments) {
		dest := args.Get(0).(*[]map[string]interface{})
		*dest = expectedLogs
	})

	// Tạo mysqlStory với DB là mock DB
	mysqlStory := &database.MysqlStory{
		DB: db,
	}

	// Gọi hàm PushLogBySchedule() và kiểm tra kết quả
	ctx := context.Background()
	mysqlStory.PushLogBySchedule(writer, ctx)

	// Kiểm tra Kafka writer đã được gọi đúng cách
	assert.Equal(t, expectedLogs, writer.Messages())
}
