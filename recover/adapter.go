package recover

import (
	"context"
	"errors"
	"eventstorming/utils/utilsredis"
	"time"

	"github.com/go-redis/redis"
)

func NewEventFailureRecover(streams []string, Log func(functionName string, err error)) (EventFailureRecover, error) {
	redisClient, err := utilsredis.RedisDBLogin()
	if err != nil {
		return RedisAdapterEventFailureRecover{}, err
	}
	consumer, err := utilsredis.RedisStreamConsumer()
	if err != nil {
		return RedisAdapterEventFailureRecover{}, err
	}
	group, err := utilsredis.RedisStreamGroup()
	if err != nil {
		return RedisAdapterEventFailureRecover{}, err
	}
	envStreams := []string{}
	for _, stream := range streams {
		if stream == "" {
			return RedisAdapterEventFailureRecover{}, errors.New("missing stream name")
		}
		envStreams = append(envStreams, stream)
	}
	return RedisAdapterEventFailureRecover{
		Client:   redisClient,
		Streams:  envStreams,
		Group:    group,
		Consumer: consumer,
		MinIdle:  5 * time.Minute,
		Log:      Log,
	}, nil
}

type RedisAdapterEventFailureRecover struct {
	Client   *redis.Client
	Streams  []string
	Group    string
	Consumer string
	Log      func(functionName string, err error)
	MinIdle  time.Duration
}

func (adapter RedisAdapterEventFailureRecover) Recover(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			for _, stream := range adapter.Streams {
				pendingExts, err := adapter.Client.XPendingExt(&redis.XPendingExtArgs{
					Stream: stream,
					Group:  adapter.Group,
					Start:  "-",
					End:    "+",
					Count:  1000,
				}).Result()
				if err != nil {
					adapter.Log("redis.XPendingExt", err)
					continue
				}
				ids := []string{}
				for _, pendingExt := range pendingExts {
					if pendingExt.Consumer != adapter.Consumer {
						ids = append(ids, pendingExt.ID)
					}
				}
				if len(ids) > 0 {
					if err := adapter.Client.XClaim(&redis.XClaimArgs{
						Stream:   stream,
						Group:    adapter.Group,
						MinIdle:  adapter.MinIdle,
						Consumer: adapter.Consumer,
						Messages: ids,
					}).Err(); err != nil {
						adapter.Log("redis.XClaim", err)
					}
				}
			}
			time.Sleep(adapter.MinIdle)
		}
	}
}
