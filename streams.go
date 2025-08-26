package redis

import (
	"context"
	"log"
	"time"
)

func (c *client) Pub(topic string, message []byte) error {
	return c.redisClient.Publish(context.Background(), topic, message).Err()
}

func (c *client) Sub(ctx context.Context, topic string, callback func(data []byte)) {
	go func() {
		var attempt int

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			pubsub := c.redisClient.Subscribe(ctx, topic)

			if _, err := pubsub.Receive(ctx); err != nil {
				log.Printf("Redis subscribe error: %v", err)
				_ = pubsub.Close()
				attempt++
				time.Sleep(backoffDuration(attempt))
				continue
			}

			attempt = 0
			ch := pubsub.Channel()

			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						goto RESUB
					}
					if msg != nil {
						callback([]byte(msg.Payload))
					}
				case <-ctx.Done():
					_ = pubsub.Close()
					return
				}
			}

		RESUB:
			_ = pubsub.Close()
			attempt++
			time.Sleep(backoffDuration(attempt))
		}
	}()
}

func backoffDuration(attempt int) time.Duration {
	if attempt <= 0 {
		return 500 * time.Millisecond
	}

	d := time.Duration(500*(1<<attempt)) * time.Millisecond
	if d > 10*time.Second {
		d = 10 * time.Second
	}
	return d
}
