package redis

import (
	"crypto/tls"
	"log"
	"os"
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
}

type client struct {
	readClient  *redis.Client
	writeClient *redis.Client
	prefix      string
}

func NewClient(prefix ...string) (redisRepository CacheRepository, err error) {
	var redisOnce sync.Once

	readAddress := getReadAddress()
	writeAddress := getWriteAddress()

	redisOnce.Do(func() {
		readClient := redis.NewClient(&redis.Options{
			Addr:     readAddress,
			PoolSize: 50,
			TLSConfig: &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true,
			},
		})

		writeClient := redis.NewClient(&redis.Options{
			Addr:     writeAddress,
			PoolSize: 50,
			TLSConfig: &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true,
			},
		})

		err = readClient.Ping().Err()
		if err != nil {
			return
		}

		err = writeClient.Ping().Err()
		if err != nil {
			return
		}

		prefixValue := ""

		if len(prefix) > 0 {
			prefixValue = prefix[0]
		}

		redisRepository = &client{
			readClient:  readClient,
			writeClient: writeClient,
			prefix:      prefixValue,
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
	result := c.readClient.Get(c.prefix + key)

	return result.Bytes()
}

func (c *client) GetHash(key, hashKey string) ([]byte, error) {
	result := c.readClient.HGet(c.prefix+key, hashKey)

	return result.Bytes()
}

func (c *client) Set(key string, value []byte, expiration time.Duration) error {
	return c.writeClient.Set(c.prefix+key, value, expiration).Err()
}

func (c *client) SetHash(key, hashKey string, value []byte) error {
	return c.writeClient.HSet(c.prefix+key, hashKey, value).Err()
}

func (c *client) Exists(key string) (bool, error) {
	e, err := c.readClient.Exists(c.prefix + key).Result()
	if err != nil {
		return false, err
	}

	return e == 1, nil
}

func (c *client) ExistsHash(key, hashKey string) (bool, error) {
	e, err := c.readClient.HExists(c.prefix+key, hashKey).Result()
	if err != nil {
		return false, err
	}

	return e, nil
}

func (c *client) GetMultipleHashKeys(key string, hashKeys []string) (map[string][]byte, error) {
	results := make(map[string][]byte)

	for _, hKey := range hashKeys {
		value, err := c.readClient.HGet(c.prefix+key, hKey).Bytes()
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
	return c.writeClient.Del(c.prefix + key).Err()
}

func (c *client) DeleteHash(key, hashKey string) error {
	return c.writeClient.HDel(c.prefix+key, hashKey).Err()
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
		scannedKeys, cursor, err = c.readClient.Scan(cursor, fullPrefix, pageSize).Result()
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
		_, err := c.writeClient.Del(keys...).Result()
		if err != nil {
			return err
		}
	}

	return nil
}
