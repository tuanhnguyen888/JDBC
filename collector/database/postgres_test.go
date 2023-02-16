package database

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"testing"
)

func TestNewPostgres(t *testing.T) {
	dsn := "host=localhost user=test password=test dbname=test port=5432 sslmode=disable"
	query := "SELECT * FROM test_table"

	db := NewPostgres(dsn, query)

	if db == nil {
		t.Errorf("Expected NewPostgres() to return a non-nil value, but got nil")
	}

	// Kiểm tra kiểu trả về
	if _, ok := db.(*postgresqlStory); !ok {
		t.Errorf("Expected NewPostgres() to return a *postgresqlStory, but got %T", db)
	}

	// Kiểm tra giá trị các thuộc tính
	ps, _ := db.(*postgresqlStory)
	if ps.query != query {
		t.Errorf("Expected query = %q, but got %q", query, ps.query)
	}
}

func TestPostgresqlStory_Execute(t *testing.T) {
	// Tạo database tạm thời dùng cho unit test
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}

	// Tạo bảng test và chèn dữ liệu vào trong đó
	if err := db.Exec("CREATE TABLE servers (id INTEGER, name TEXT)").Error; err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	if err := db.Exec("INSERT INTO servers (id, name) VALUES (?, ?), (?, ?)", 1, "server1", 2, "server2").Error; err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Tạo một đối tượng postgresqlStory mới với database tạm thời và truy vấn test
	story := &postgresqlStory{
		query: "SELECT * FROM servers",
		DB:    db,
	}
	logs, err := story.Execute()
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}

	// Kiểm tra kết quả trả về
	expectedLogs := `[{"id":1,"name":"server1"},{"id":2,"name":"server2"}]`
	if string(logs) != expectedLogs {
		t.Errorf("unexpected result, expected: %v, got: %v", expectedLogs, string(logs))
	}
}
