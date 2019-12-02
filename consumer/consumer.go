package consumer

import (
	"context"
	"github.com/bagus212/eventstorming"
)

type EventConsumer interface {
	Run(ctx context.Context) error
}

type EventConsumerHandler interface {
	Apply(event eventstorming.Event) error
}
