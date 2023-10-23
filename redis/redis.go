package redis

import (
	"log"
	"sync"

	"github.com/go-redis/redis"
)

type CacheRepository interface {
	Get(key, hashKey string) ([]byte, error)
	Set(key, hashKey string, value interface{}) error
	Exists(key string) (bool, error)
	ExistsHash(key, hashKey string) (bool, error)
}

type client struct {
	Client *redis.Client
}

func NewRedisClient(address, password string) CacheRepository {
	var redisClient CacheRepository
	var redisOnce sync.Once

	redisOnce.Do(func() {
		c := redis.NewClient(&redis.Options{
			Addr:     address,
			Password: password,
			DB:       0,
		})

		if err := c.Ping().Err(); err != nil {
			log.Fatal("Error connecting to redis: ", err)
			return
		}

		redisClient = &client{
			Client: c,
		}
	})

	return redisClient
}

func (c *client) Get(key, hashKey string) ([]byte, error) {
	result := c.Client.HGet(key, hashKey)

	return result.Bytes()
}

func (c *client) Set(key, hashKey string, value interface{}) error {
	return c.Client.HSet(key, hashKey, value).Err()
}

func (c *client) Exists(key string) (bool, error) {
	e, err := c.Client.Exists(key).Result()
	if err != nil {
		return false, err
	}

	return e == 1, nil
}

func (c *client) ExistsHash(key, hashKey string) (bool, error) {
	e, err := c.Client.HExists(key, hashKey).Result()
	if err != nil {
		return false, err
	}

	return e, nil
}
