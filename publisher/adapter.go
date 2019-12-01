package publisher

import (
	"eventstorming"
	"eventstorming/eventstore"

	"github.com/go-redis/redis"
)

type RedisAdapterDomainEventPublisher struct {
	Client           *redis.Client
	Channel          string
	SaveToEventStore bool
	EventStore       eventstore.EventStore
}

func (adapter RedisAdapterDomainEventPublisher) Publish(event eventstorming.Event) error {
	if adapter.SaveToEventStore {
		if err := adapter.EventStore.Save(&event); err != nil {
			return err
		}
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
