package cache

import (
	"errors"
	"github.com/go-redis/redis"
)

type redisConn struct {
	Conn *redis.Client
}

var redisC *redisConn

func NewInitRedis() (*redisConn, error) {
	if redisC == nil {
		redisClient := redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		})
		if redisClient == nil {
			return nil, errors.New("can not connect redis")
		}
		redisC = &redisConn{
			Conn: redisClient,
		}

		return redisC, nil
	}

	return redisC, nil
}
