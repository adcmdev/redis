package redis

import (
	"context"
	"time"
)

func (c *client) Pub(topic string, message []byte) error {
	return c.redisClient.Publish(topic, message).Err()
}

func (c *client) Sub(ctx context.Context, topic string, callback func(data []byte)) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			pubsub := c.redisClient.Subscribe(topic)
			ch := pubsub.Channel()

			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						goto RESUB
					}
					callback([]byte(msg.Payload))
				case <-ctx.Done():
					_ = pubsub.Close()
					return
				}
			}
		RESUB:
			_ = pubsub.Close()
			time.Sleep(2 * time.Second)
		}
	}()
}
