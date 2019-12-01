package consumer

import (
	"context"
	"eventstorming"
)

type EventConsumer interface {
	Run(ctx context.Context)
}

type EventConsumerHandler interface {
	Apply(event eventstorming.Event) error
}
