package redis

import (
	"log"
	"os"
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
	ReadClient  *redis.Client
	WriteClient *redis.Client
}

func NewClient() (redisRepository CacheRepository, err error) {
	var redisOnce sync.Once

	readAddress := getReadAddress()
	writeAddress := getWriteAddress()

	redisOnce.Do(func() {
		readClient := redis.NewClient(&redis.Options{
			Addr:     readAddress,
			Password: "",
			DB:       0,
		})

		writeClient := redis.NewClient(&redis.Options{
			Addr:     writeAddress,
			Password: "",
			DB:       0,
		})

		err = readClient.Ping().Err()
		if err != nil {
			return
		}

		err = writeClient.Ping().Err()
		if err != nil {
			return
		}

		redisRepository = &client{
			ReadClient:  readClient,
			WriteClient: writeClient,
		}
	})

	return
}

func getReadAddress() string {
	host := os.Getenv("GLOBAL_REDIS_HOST_READ")
	if host == "" {
		host = "localhost"
	}

	return host + ":6379"
}

func getWriteAddress() string {
	host := os.Getenv("GLOBAL_REDIS_HOST")
	if host == "" {
		host = "localhost"
	}

	return host + ":6379"
}

func (c *client) Get(key string) ([]byte, error) {
	result := c.ReadClient.Get(key)

	return result.Bytes()
}

func (c *client) GetHash(key, hashKey string) ([]byte, error) {
	result := c.ReadClient.HGet(key, hashKey)

	return result.Bytes()
}

func (c *client) Set(key string, value []byte, expiration time.Duration) error {
	return c.WriteClient.Set(key, value, expiration).Err()
}

func (c *client) SetHash(key, hashKey string, value []byte) error {
	return c.WriteClient.HSet(key, hashKey, value).Err()
}

func (c *client) Exists(key string) (bool, error) {
	e, err := c.ReadClient.Exists(key).Result()
	if err != nil {
		return false, err
	}

	return e == 1, nil
}

func (c *client) ExistsHash(key, hashKey string) (bool, error) {
	e, err := c.ReadClient.HExists(key, hashKey).Result()
	if err != nil {
		return false, err
	}

	return e, nil
}

func (c *client) GetMultipleHashKeys(key string, hashKeys []string) (map[string][]byte, error) {
	results := make(map[string][]byte)

	for _, hKey := range hashKeys {
		value, err := c.ReadClient.HGet(key, hKey).Bytes()
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
