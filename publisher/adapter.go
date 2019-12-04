package publisher

import (
	"github.com/bagus212/eventstorming"
	"github.com/bagus212/eventstorming/eventstore"
	"github.com/bagus212/eventstorming/utils"

	"github.com/go-redis/redis"
)

type RedisAdapterDomainEventPublisher struct {
	RedisUrl         string
	DBIndex          string
	Password         string
	Channel          string
	SaveToEventStore bool
	EventStore       eventstore.EventStore
	client           *redis.Client
}

func (adapter *RedisAdapterDomainEventPublisher) Publish(event eventstorming.Event) error {
	if err := adapter.init(); err != nil {
		return err
	}
	if err := adapter.save(event); err != nil {
		return err
	}
	return adapter.publish(event)
}

func (adapter *RedisAdapterDomainEventPublisher) init() error {
	if adapter.client == nil {
		client, err := utils.RedisDBLogin(adapter.RedisUrl, adapter.DBIndex, adapter.Password)
		if err != nil {
			return err
		}
		adapter.client = client
	}
	return nil
}

func (adapter *RedisAdapterDomainEventPublisher) save(event eventstorming.Event) error {
	if adapter.SaveToEventStore && adapter.EventStore != nil {
		if err := adapter.EventStore.Save(event); err != nil {
			return err
		}
	}
	return nil
}

func (adapter *RedisAdapterDomainEventPublisher) publish(event eventstorming.Event) error {
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
	if err := adapter.client.XAdd(&redis.XAddArgs{
		Stream: adapter.Channel,
		ID:     "*",
		Values: values,
	}).Err(); err != nil {
		return err
	}
	return nil
}
