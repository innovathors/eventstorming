package utilsredis

import (
	"os"
	"strconv"

	"errors"

	"github.com/go-redis/redis"
)

const (
	REDIS_ADDRESS      = "REDIS_ADDRESS"
	REDIS_PASSWORD     = "REDIS_PASSWORD"
	REDIS_DB           = "REDIS_DB"
	REDIS_STREAM_GROUP = "REDIS_STREAM_GROUP"
)

func RedisDBLogin() (*redis.Client, error) {
	redisURL := os.Getenv(REDIS_ADDRESS)
	redisDB := os.Getenv(REDIS_DB)
	redisDBIndex, err := strconv.Atoi(redisDB)
	if err != nil {
		return nil, err
	}
	redisPass := os.Getenv(REDIS_PASSWORD)
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		Password: redisPass,
		DB:       redisDBIndex,
	})
	err = redisClient.Ping().Err()
	return redisClient, err
}

func RedisStreamGroup() (string, error) {
	redisStreamGroup := os.Getenv(REDIS_STREAM_GROUP)
	if redisStreamGroup == "" {
		return "", errors.New("missing redis group name")
	}
	return redisStreamGroup, nil
}

func RedisStreamConsumer() (string, error) {
	redisStreamConsumer, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return redisStreamConsumer, nil
}
