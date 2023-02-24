package cache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewInitRedis(t *testing.T) {
	redisConn, err := NewInitRedis()

	// Kiểm tra lỗi trả về
	assert.NoError(t, err)

	// Kiểm tra đối tượng trả về có phải là *redisConn không
	assert.IsType(t, redisConn, redisConn)

	// Kiểm tra đối tượng redis.Client không nil
	assert.NotNil(t, redisConn.Conn)
}
