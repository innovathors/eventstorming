package eventstore

import "github.com/bagus212/eventstorming"

type EventStore interface {
	Save(event eventstorming.Event) error
}
