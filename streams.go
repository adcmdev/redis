package redis

import (
	"encoding/json"
	"log"
	"time"

	"github.com/go-redis/redis"
)

// Emit serializa el mensaje a JSON y lo envía como []byte bajo la clave "data".
func (c *client) Emit(topic string, message []byte) error {
	return c.redisClient.XAdd(&redis.XAddArgs{
		Stream: topic,
		Values: map[string]interface{}{
			"data": message,
		},
	}).Err()
}

// Listen escucha el stream y deserializa el campo "data" como []byte.
func (c *client) Listen(topic string, callback func(data []byte)) {
	lastID := "$"

	for {
		streams, err := c.redisClient.XRead(&redis.XReadArgs{
			Streams: []string{topic, lastID},
			Count:   1,
			Block:   5 * time.Second,
		}).Result()

		if err != nil {
			if err == redis.Nil {
				continue
			}

			log.Printf("Error reading from stream: %s", err)
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				rawData, ok := msg.Values["data"]
				if !ok {
					log.Println("Missing 'data' field in message")
					continue
				}

				var data []byte
				switch v := rawData.(type) {
				case string:
					data = []byte(v)
				case []byte:
					data = v
				default:
					// Intentar convertir a JSON y luego a []byte
					jsonData, err := json.Marshal(v)
					if err != nil {
						log.Printf("Invalid message data: %v", err)
						continue
					}
					data = jsonData
				}

				callback(data)
				lastID = msg.ID
			}
		}
	}
}
