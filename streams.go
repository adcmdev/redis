package redis

import (
	"log"
	"time"

	"github.com/go-redis/redis"
)

func (c *client) Emit(topic string, message map[string]interface{}) error {
	err := c.redisClient.XAdd(
		&redis.XAddArgs{
			Stream: topic,
			Values: message,
		},
	).Err()

	return err
}
func (c *client) Listen(topic string, callback func(values map[string]interface{})) {
	lastID := "$"

	for {
		streams, err := c.redisClient.XRead(
			&redis.XReadArgs{
				Streams: []string{topic, lastID},
				Count:   1,
				Block:   5 * time.Second,
			},
		).Result()

		if err != nil {
			if err == redis.Nil {
				continue
			}

			log.Printf("Error reading from stream: %s", err)
			continue
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				callback(message.Values)
				lastID = message.ID
			}
		}
	}
}
