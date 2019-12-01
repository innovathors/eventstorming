package publisher

import "eventstorming"

type DomainEventPublisher interface {
	Publish(event eventstorming.Event) error
}
