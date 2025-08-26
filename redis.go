package redis

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
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

	// Pub/Sub
	Pub(topic string, message []byte) error
	Sub(ctx context.Context, topic string, callback func(data []byte))
	Close() error
}

type client struct {
	redisClient *redis.Client
	prefix      string
}

var (
	once           sync.Once
	clientInstance *client
	initErr        error
)

func NewClient(dto CreateNewRedisDTO) (CacheRepository, error) {
	once.Do(func() {
		address := getAddress(dto.Host)
		if dto.Network == "" {
			dto.Network = "tcp"
		}

		rdb := redis.NewClient(&redis.Options{
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

			PoolSize:     10 * runtime.NumCPU(),
			MinIdleConns: 2 * runtime.NumCPU(),
			PoolTimeout:  30 * time.Second,

			TLSConfig: dto.TLSConfig,
			OnConnect: func(ctx context.Context, cn *redis.Conn) error {
				return nil
			},
		})

		if err := rdb.Ping(context.Background()).Err(); err != nil {
			initErr = err
			return
		}

		clientInstance = &client{
			redisClient: rdb,
			prefix:      dto.Prefix,
		}
	})

	return clientInstance, initErr
}

func getAddress(host string) string {
	const defaultPort = "6379"
	const defaultHost = "localhost:" + defaultPort

	if host != "" {
		return host
	}

	host = os.Getenv("REDIS_HOST")
	if host == "" {
		return defaultHost
	}

	return host + ":" + defaultPort
}

func (c *client) Get(key string) ([]byte, error) {
	return c.redisClient.Get(context.Background(), c.prefix+key).Bytes()
}

func (c *client) GetHash(key, hashKey string) ([]byte, error) {
	return c.redisClient.HGet(context.Background(), c.prefix+key, hashKey).Bytes()
}

func (c *client) Set(key string, value []byte, expiration time.Duration) error {
	return c.redisClient.Set(context.Background(), c.prefix+key, value, expiration).Err()
}

func (c *client) SetHash(key, hashKey string, value []byte) error {
	return c.redisClient.HSet(context.Background(), c.prefix+key, hashKey, value).Err()
}

func (c *client) Exists(key string) (bool, error) {
	e, err := c.redisClient.Exists(context.Background(), c.prefix+key).Result()
	if err != nil {
		return false, err
	}

	return e > 0, nil
}

func (c *client) ExistsHash(key, hashKey string) (bool, error) {
	return c.redisClient.HExists(context.Background(), c.prefix+key, hashKey).Result()
}

func (c *client) GetMultipleHashKeys(key string, hashKeys []string) (map[string][]byte, error) {
	values, err := c.redisClient.HMGet(context.Background(), c.prefix+key, hashKeys...).Result()
	if err != nil {
		return nil, err
	}

	results := make(map[string][]byte)
	for i, v := range values {
		if v == nil {
			continue
		}
		switch val := v.(type) {
		case string:
			results[hashKeys[i]] = []byte(val)
		case []byte:
			results[hashKeys[i]] = val
		default:
			results[hashKeys[i]] = []byte(fmt.Sprint(val))
		}
	}

	return results, nil
}

func (c *client) Delete(key string) error {
	return c.redisClient.Del(context.Background(), c.prefix+key).Err()
}

func (c *client) DeleteHash(key, hashKey string) error {
	return c.redisClient.HDel(context.Background(), c.prefix+key, hashKey).Err()
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
		scannedKeys, cursor, err = c.redisClient.Scan(context.Background(), cursor, fullPrefix, pageSize).Result()
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
		_, err := c.redisClient.Del(context.Background(), keys...).Result()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *client) Close() error {
	return c.redisClient.Close()
}
