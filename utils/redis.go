package utils

import (
	"strconv"

	"github.com/go-redis/redis"
)

func RedisDBLogin(url, db, password string) (*redis.Client, error) {
	redisDBIndex, err := strconv.Atoi(db)
	if err != nil {
		return nil, err
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:     url,
		Password: password,
		DB:       redisDBIndex,
	})
	err = redisClient.Ping().Err()
	return redisClient, err
}
