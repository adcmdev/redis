package redis

import (
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type CacheRepository interface {
	Get(key string) ([]byte, error)
	GetHash(key, hashKey string) ([]byte, error)
	Set(key string, value []byte, expiration time.Duration) error
	SetHash(key, hashKey string, value []byte) error
	Exists(key string) (bool, error)
	ExistsHash(key, hashKey string) (bool, error)
	GetMultipleHashKeys(key string, hashKeys []string) (map[string][]byte, error)
}

type client struct {
	Client *redis.Client
}

func NewClient(address, password string) (CacheRepository, error) {
	var redisClient CacheRepository
	var redisOnce sync.Once
	var err error

	redisOnce.Do(func() {
		c := redis.NewClient(&redis.Options{
			Addr:     address,
			Password: password,
			DB:       0,
		})

		err = c.Ping().Err()

		if err != nil {
			log.Fatal("Error connecting to redis: ", err)
			return
		}

		redisClient = &client{
			Client: c,
		}
	})

	if err != nil {
		return nil, err
	}

	return redisClient, nil
}

func (c *client) Get(key string) ([]byte, error) {
	result := c.Client.Get(key)

	return result.Bytes()
}

func (c *client) GetHash(key, hashKey string) ([]byte, error) {
	result := c.Client.HGet(key, hashKey)

	return result.Bytes()
}

func (c *client) Set(key string, value []byte, expiration time.Duration) error {
	return c.Client.Set(key, value, expiration).Err()
}

func (c *client) SetHash(key, hashKey string, value []byte) error {
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

func (c *client) GetMultipleHashKeys(key string, hashKeys []string) (map[string][]byte, error) {
	results := make(map[string][]byte)

	for _, hKey := range hashKeys {
		value, err := c.Client.HGet(key, hKey).Bytes()
		if err == redis.Nil {
			continue
		}

		if err != nil {
			log.Fatal("error getting hash key: ", err)
			return nil, err
		}

		results[hKey] = value
	}

	return results, nil
}
