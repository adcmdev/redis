package redis

import (
	"context"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type CacheRepository interface {
	Get(key string) ([]byte, error)
	GetAllKeys(prefix string, size ...int) ([]string, error)
	GetHash(key, hashKey string) ([]byte, error)
	Set(key string, value []byte, expiration time.Duration) error
	SetHash(key, hashKey string, value []byte) error
	Exists(key string) (bool, error)
	ExistsHash(key, hashKey string) (bool, error)
	GetMultipleHashKeys(key string, hashKeys []string) (map[string][]byte, error)
	Delete(key string) error
	DeleteHash(key, hashKey string) error
	DeleteAll(prefix string) error

	// Streams
	Emit(topicPrefix, partitionKey string, message []byte, shards int) error
	ListenGroup(ctx context.Context, topic, group string, callback func(data [][]byte)) error

	// Pub/Sub
	Pub(topic string, message []byte) error
	Sub(ctx context.Context, topic string, callback func(data []byte))
}

type client struct {
	redisClient *redis.Client
	prefix      string
}

func NewClient(dto CreateNewRedisDTO) (CacheRepository, error) {
	var redisOnce sync.Once
	var redisRepository CacheRepository
	var err error

	address := getAddress(dto.Host)
	if dto.Network == "" {
		dto.Network = "tcp"
	}

	redisOnce.Do(func() {
		readClient := redis.NewClient(&redis.Options{
			Network:  dto.Network,
			Addr:     address,
			Password: dto.Password,
			DB:       dto.DB,

			DialTimeout:  10 * time.Second,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,

			MaxRetries:      5,
			MinRetryBackoff: 50 * time.Millisecond,
			MaxRetryBackoff: 2 * time.Second,

			PoolSize:     64 * runtime.NumCPU(),
			MinIdleConns: 16 * runtime.NumCPU(),
			IdleTimeout:  5 * time.Minute,
			MaxConnAge:   30 * time.Minute,
			PoolTimeout:  30 * time.Second,

			IdleCheckFrequency: time.Minute,
			TLSConfig:          dto.TLSConfig,
			OnConnect: func(conn *redis.Conn) error {
				return nil
			},
		})

		err = readClient.Ping().Err()

		if err != nil {
			return
		}

		redisRepository = &client{
			redisClient: readClient,
			prefix:      dto.Prefix,
		}
	})

	return redisRepository, err
}

func getAddress(host string) string {
	const defaultPort = "6379"
	const defaultHost = "localhost" + ":" + defaultPort

	if host != "" {
		return host
	}

	if host == "" {
		return defaultHost
	}

	host = os.Getenv("REDIS_HOST")
	if host == "" {
		return defaultHost
	}

	return host + ":" + defaultPort
}

func (c *client) Get(key string) ([]byte, error) {
	result := c.redisClient.Get(c.prefix + key)

	return result.Bytes()
}

func (c *client) GetHash(key, hashKey string) ([]byte, error) {
	result := c.redisClient.HGet(c.prefix+key, hashKey)

	return result.Bytes()
}

func (c *client) Set(key string, value []byte, expiration time.Duration) error {
	return c.redisClient.Set(c.prefix+key, value, expiration).Err()
}

func (c *client) SetHash(key, hashKey string, value []byte) error {
	return c.redisClient.HSet(c.prefix+key, hashKey, value).Err()
}

func (c *client) Exists(key string) (bool, error) {
	e, err := c.redisClient.Exists(c.prefix + key).Result()
	if err != nil {
		return false, err
	}

	return e == 1, nil
}

func (c *client) ExistsHash(key, hashKey string) (bool, error) {
	e, err := c.redisClient.HExists(c.prefix+key, hashKey).Result()
	if err != nil {
		return false, err
	}

	return e, nil
}

func (c *client) GetMultipleHashKeys(key string, hashKeys []string) (map[string][]byte, error) {
	results := make(map[string][]byte)

	for _, hKey := range hashKeys {
		value, err := c.redisClient.HGet(c.prefix+key, hKey).Bytes()
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

func (c *client) Delete(key string) error {
	return c.redisClient.Del(c.prefix + key).Err()
}

func (c *client) DeleteHash(key, hashKey string) error {
	return c.redisClient.HDel(c.prefix+key, hashKey).Err()
}

func (c *client) GetAllKeys(prefix string, size ...int) ([]string, error) {
	var pageSize int64

	if len(size) > 0 {
		pageSize = int64(size[0])
	} else {
		pageSize = 10
	}

	fullPrefix := c.prefix + prefix + "*"

	var cursor uint64
	var keys []string
	var err error

	for {
		var scannedKeys []string
		scannedKeys, cursor, err = c.redisClient.Scan(cursor, fullPrefix, pageSize).Result()
		if err != nil {
			return nil, err
		}

		if len(scannedKeys) > 0 {
			keys = append(keys, scannedKeys...)
		}

		if cursor == 0 {
			break
		}
	}

	return keys, nil
}

func (c *client) DeleteAll(prefix string) error {
	keys, err := c.GetAllKeys(prefix)
	if err != nil {
		return err
	}

	if len(keys) > 0 {
		_, err := c.redisClient.Del(keys...).Result()
		if err != nil {
			return err
		}
	}

	return nil
}
