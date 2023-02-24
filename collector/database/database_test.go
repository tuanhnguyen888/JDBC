package database

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetNameTableByQuery(t *testing.T) {
	// Test case 1: Truy vấn chứa tên bảng hợp lệ
	query := "SELECT * FROM users WHERE id = 1"
	tableName, err := getNameTableByQuery(query)

	// Kiểm tra lỗi
	assert.Nil(t, err)

	// Kiểm tra kết quả
	assert.Equal(t, "users", tableName)

	// Test case 2: Truy vấn không chứa tên bảng
	query = "SELECT * FROM WHERE id = 1"
	tableName, err = getNameTableByQuery(query)

	// Kiểm tra lỗi
	assert.NotNil(t, err)

	// Kiểm tra kết quả
	assert.Equal(t, "", tableName)
}
