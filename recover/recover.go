package recover

import (
	"context"
)

type EventFailureRecover interface {
	Recover(ctx context.Context)
}
