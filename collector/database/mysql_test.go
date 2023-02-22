package database_test

import (
	"github.com/golang/mock/gomock"
	"gorm.io/gorm"
	"testing"
)

type MockDB interface {
	gorm.DB
}

func TestNewMysql(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB = mocks.New(ctrl)
	dsn := "testdsn"
	query := "testquery"

	mockDB.EXPECT().Ping()

	NewMysql(dsn, query)
}
