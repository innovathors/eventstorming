package publisher

import (
	"github.com/bagus212/eventstorming"
)

type PublisherSuccessStub struct{}

func (mock *PublisherSuccessStub) Publish(event eventstorming.Event) error {
	return nil
}

type PublisherErrorStub struct{}

func (mock *PublisherErrorStub) Publish(event eventstorming.Event) error {
	return nil
}
