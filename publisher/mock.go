package publisher

import (
	"github.com/innovathors/eventstorming"
	"github.com/stretchr/testify/mock"
)

type PublisherMock struct {
	mock.Mock
}

func (mock *PublisherMock) Publish(event eventstorming.Event) error {
	args := mock.Called(event)
	return args.Error(0)
}
