package eventstore

import "github.com/innovathors/eventstorming"

type EventStore interface {
	Save(event eventstorming.Event) error
}
