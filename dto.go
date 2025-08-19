package redis

import (
	"crypto/tls"
	"time"

	"github.com/go-redis/redis"
)

type CreateNewRedisDTO struct {
	Host               string
	Prefix             string
	Network            string
	Password           string
	DB                 int
	MaxRetries         int
	MinRetryBackoff    time.Duration
	MaxRetryBackoff    time.Duration
	DialTimeout        time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	PoolSize           int
	MinIdleConns       int
	MaxConnAge         time.Duration
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
	TLSConfig          *tls.Config
	OnConnect          func(*redis.Conn) error
}
