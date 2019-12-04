package consumer

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/innovathors/eventstorming/utils"

	"github.com/innovathors/eventstorming"

	"context"

	"github.com/go-redis/redis"
)

type RedisEventConsumer struct {
	RedisUrl         string
	DBIndex          string
	Password         string
	Group            string
	Consumer         string
	NewMessage       bool
	Foreground       bool
	HandlerConsumers map[string]EventConsumerHandler
	LogFunction      func(functionName string, message string)
}

func (adapter RedisEventConsumer) Run(ctx context.Context) error {
	client, err := utils.RedisDBLogin(adapter.RedisUrl, adapter.DBIndex, adapter.Password)
	if err != nil {
		return err
	}
	streams, err := adapter.initStream(client)
	if err != nil {
		return err
	}
	go adapter.recover(streams, ctx, client)
	if adapter.Foreground {
		adapter.consume(streams, ctx, client)
	} else {
		go adapter.consume(streams, ctx, client)
	}
	return nil
}

func (adapter RedisEventConsumer) consume(streams []string, ctx context.Context, client *redis.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, err := client.XReadGroup(&redis.XReadGroupArgs{
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
					err = client.XAck(stream.Stream, adapter.Group, message.ID).Err()
					if err != nil {
						adapter.log("redis.XAck", err.Error())
						continue
					}
				}
			}
		}
	}
}

func (adapter RedisEventConsumer) recover(streams []string, ctx context.Context, client *redis.Client) {
	minIdle := time.Minute
	for {
		select {
		case <-ctx.Done():
			return
		default:
			for _, stream := range streams {
				pendingExts, err := client.XPendingExt(&redis.XPendingExtArgs{
					Stream: stream,
					Group:  adapter.Group,
					Start:  "-",
					End:    "+",
					Count:  1000,
				}).Result()
				if err != nil {
					adapter.log("redis.XPendingExt", err.Error())
					continue
				}
				ids := []string{}
				for _, pendingExt := range pendingExts {
					if pendingExt.Consumer != adapter.Consumer {
						ids = append(ids, pendingExt.ID)
					}
				}
				if len(ids) > 0 {
					if err := client.XClaim(&redis.XClaimArgs{
						Stream:   stream,
						Group:    adapter.Group,
						MinIdle:  minIdle,
						Consumer: adapter.Consumer,
						Messages: ids,
					}).Err(); err != nil {
						adapter.log("redis.XClaim", err.Error())
					}
				}
			}
			time.Sleep(minIdle)
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

func (adapter RedisEventConsumer) initStream(client *redis.Client) ([]string, error) {
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
		if err := client.XGroupCreateMkStream(stream, adapter.Group, startMessage).Err(); err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
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
