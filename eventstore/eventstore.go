package eventstore

import "eventstorming"

type EventStore interface {
	Save(event *eventstorming.Event) error
}
