package publisher

import "github.com/innovathors/eventstorming"

type DomainEventPublisher interface {
	Publish(event eventstorming.Event) error
}
