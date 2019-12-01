package consumer

import (
	"encoding/json"
	"errors"
	"eventstorming"
	"log"
	"time"

	"context"

	"github.com/go-redis/redis"
)

type RedisEventConsumer struct {
	Client           *redis.Client
	HandlerConsumers map[string]EventConsumerHandler
	Group            string
	Consumer         string
	NewMessage       bool
	Foreground       bool
	LogFunction      func(functionName string, message string)
}

func (adapter RedisEventConsumer) Run(ctx context.Context) error {
	streams, err := adapter.initStream()
	if err != nil {
		return err
	}
	if adapter.Foreground {
		adapter.start(streams, ctx)
	} else {
		go adapter.start(streams, ctx)
	}
	return nil
}

func (adapter RedisEventConsumer) start(streams []string, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, err := adapter.Client.XReadGroup(&redis.XReadGroupArgs{
				Streams:  streams,
				Group:    adapter.Group,
				Consumer: adapter.Consumer,
				NoAck:    false,
				Count:    100,
				Block:    time.Minute,
			}).Result()
			if err != nil {
				adapter.log("redis.XReadGroup", err.Error())
				continue
			}
			for _, stream := range streams {
				for _, message := range stream.Messages {
					event, err := adapter.messageToEvent(message.Values)
					if err != nil {
						adapter.log("adapter.messageToEvent", err.Error())
						continue
					}
					err = adapter.HandlerConsumers[stream.Stream].Apply(event)
					if err != nil {
						adapter.log("adapter.HandlerConsumers[stream.Stream].Apply(event)", err.Error())
						continue
					}
					err = adapter.Client.XAck(stream.Stream, adapter.Group, message.ID).Err()
					if err != nil {
						adapter.log("redis.XAck", err.Error())
						continue
					}
				}
			}
		}
	}
}

func (adapter RedisEventConsumer) log(functionName string, message string) {
	if adapter.LogFunction != nil {
		adapter.LogFunction(functionName, message)
	} else {
		log.Println(functionName + " : " + message)
	}
}

func (adapter RedisEventConsumer) initStream() ([]string, error) {
	var streams []string
	var startMessage string
	if adapter.NewMessage {
		startMessage = "$"
	} else {
		startMessage = "0"
	}
	for stream := range adapter.HandlerConsumers {
		if stream == "" {
			return streams, errors.New("missing redis stream name")
		}
		if err := adapter.Client.XGroupCreateMkStream(stream, adapter.Group, startMessage).Err(); err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return streams, err
		}
		streams = append(streams, stream)
	}
	for range adapter.HandlerConsumers {
		streams = append(streams, ">")
	}
	return streams, nil
}

func (adapter RedisEventConsumer) messageToEvent(values map[string]interface{}) (eventstorming.Event, error) {
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
