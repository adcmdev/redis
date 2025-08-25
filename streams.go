package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis"
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

func (c *client) Emit(topic string, message []byte) error {
	return c.redisClient.XAdd(&redis.XAddArgs{
		Stream: topic,
		Values: map[string]interface{}{"msg": message},
	}).Err()
}

func (c *client) Listen(ctx context.Context, topic string, callback func(data []byte)) {
	go func() {
		lastID := "0"
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			res, err := c.redisClient.XRead(&redis.XReadArgs{
				Streams: []string{topic, lastID},
				Count:   100,
				Block:   0,
			}).Result()
			if err != nil {
				time.Sleep(time.Second)
				continue
			}

			for _, stream := range res {
				for _, msg := range stream.Messages {
					data := msg.Values["msg"].(string)
					callback([]byte(data))
					c.redisClient.XDel(topic, msg.ID)
					lastID = msg.ID
				}
			}
		}
	}()
}
