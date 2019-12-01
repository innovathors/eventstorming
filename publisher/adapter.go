package publisher

import (
	"errors"
	"eventstorming"
	"eventstorming/eventstore"
	"eventstorming/utils/utilsredis"
	"os"

	"github.com/go-redis/redis"
)

const (
	BOOKING_CHANNEL = "BOOKING_CHANNEL"
)

func NewBookingDomainEventPublisher() (RedisAdapterDomainEventPublisher, error) {
	return NewRedisAdapterDomainEventPublisher(BOOKING_CHANNEL)
}

func NewRedisAdapterDomainEventPublisher(channelName string) (RedisAdapterDomainEventPublisher, error) {
	redisClient, err := utilsredis.RedisDBLogin()
	if err != nil {
		return RedisAdapterDomainEventPublisher{}, err
	}
	eventStore, err := eventstore.NewMongoAdapterEventStore()
	if err != nil {
		return RedisAdapterDomainEventPublisher{}, err
	}
	channel := os.Getenv(channelName)
	if channel == "" {
		return RedisAdapterDomainEventPublisher{}, errors.New("missing channel name")
	}
	return RedisAdapterDomainEventPublisher{
		Channel:    channel,
		Client:     redisClient,
		EventStore: eventStore,
	}, nil
}

type RedisAdapterDomainEventPublisher struct {
	Channel    string
	Client     *redis.Client
	EventStore eventstore.EventStore
}

func (adapter RedisAdapterDomainEventPublisher) Publish(event eventstorming.Event) error {
	if err := adapter.EventStore.Save(&event); err != nil {
		return err
	}
	values := map[string]interface{}{
		"event_id":   event.EventID,
		"event_name": event.EventName,
		"event_type": event.EventType,
		"data_id":    event.DataID,
		"data_name":  event.DataName,
		"data":       event.Data,
		"create_by":  event.CreateBy,
		"timestamp":  event.TimeStamp,
	}
	if err := adapter.Client.XAdd(&redis.XAddArgs{
		Stream: adapter.Channel,
		ID:     "*",
		Values: values,
	}).Err(); err != nil {
		return err
	}
	return nil
}
