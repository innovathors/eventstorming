package publisher

import "github.com/bagus212/eventstorming"

type DomainEventPublisher interface {
	Publish(event eventstorming.Event) error
}
