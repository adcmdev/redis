package redis

func (c *client) Pub(topic string, message []byte) error {
	return c.redisClient.Publish(topic, message).Err()
}

func (c *client) Sub(topic string, callback func(data []byte)) {
	pubsub := c.redisClient.Subscribe(topic)
	defer pubsub.Close()

	ch := pubsub.Channel()

	for msg := range ch {
		callback([]byte(msg.Payload))
	}
}
