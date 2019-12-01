package consumer

import (
	"encoding/json"
	"errors"
	"eventstorming"
	"eventstorming/utils/utilsredis"
	"time"

	"context"

	"github.com/go-redis/redis"
)

func NewRedisAdapterEventConsumer(handlerConsumers map[string]EventConsumerHandler, Log func(functionName string, err error)) (RedisAdaptersEventConsumer, error) {
	redisClient, err := utilsredis.RedisDBLogin()
	if err != nil {
		return RedisAdaptersEventConsumer{}, err
	}
	consumer, err := utilsredis.RedisStreamConsumer()
	if err != nil {
		return RedisAdaptersEventConsumer{}, err
	}
	group, err := utilsredis.RedisStreamGroup()
	if err != nil {
		return RedisAdaptersEventConsumer{}, err
	}
	var streams []string
	for key := range handlerConsumers {
		if key == "" {
			return RedisAdaptersEventConsumer{}, errors.New("missing redis stream name")
		}
		if err := redisClient.XGroupCreateMkStream(key, group, "0").Err(); err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return RedisAdaptersEventConsumer{}, err
		}
		streams = append(streams, key)
	}
	for range handlerConsumers {
		streams = append(streams, ">")
	}
	return RedisAdaptersEventConsumer{
		Client:           redisClient,
		HandlerConsumers: handlerConsumers,
		Streams:          streams,
		Group:            group,
		Consumer:         consumer,
		Log:              Log,
	}, nil
}

type RedisAdaptersEventConsumer struct {
	Client           *redis.Client
	HandlerConsumers map[string]EventConsumerHandler
	Streams          []string
	Group            string
	Consumer         string
	Log              func(functionName string, err error)
}

func (adapter RedisAdaptersEventConsumer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, err := adapter.Client.XReadGroup(&redis.XReadGroupArgs{
				Streams:  adapter.Streams,
				Group:    adapter.Group,
				Consumer: adapter.Consumer,
				NoAck:    false,
				Count:    100,
				Block:    time.Minute,
			}).Result()
			if err != nil {
				adapter.Log("redis.XReadGroup", err)
				continue
			}
			for _, stream := range streams {
				for _, message := range stream.Messages {
					event, err := adapter.messageToEvent(message.Values)
					if err != nil {
						adapter.Log("adapter.messageToEvent", err)
						continue
					}
					err = adapter.HandlerConsumers[stream.Stream].Apply(event)
					if err != nil {
						adapter.Log("adapter.HandlerConsumers[stream.Stream].Apply(event)", err)
						continue
					}
					err = adapter.Client.XAck(stream.Stream, adapter.Group, message.ID).Err()
					if err != nil {
						adapter.Log("redis.XAck", err)
						continue
					}
				}
			}
		}
	}
}

func (adapter RedisAdaptersEventConsumer) messageToEvent(values map[string]interface{}) (eventstorming.Event, error) {
	var event eventstorming.Event
	valuesString, err := json.Marshal(values)
	if err != nil {
		return event, err
	}
	err = json.Unmarshal([]byte(valuesString), &event)
	if err != nil {
		return event, err
	}
	return event, nil
}
