package redis

import (
	"context"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
)

const (
	maxLen         = 10000
	consumerPrefix = "consumer"
)

func (c *client) Pub(topic string, message []byte) error {
	return c.redisClient.Publish(topic, message).Err()
}

func (c *client) Sub(ctx context.Context, topic string, callback func(data []byte)) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			pubsub := c.redisClient.Subscribe(topic)
			ch := pubsub.Channel()

			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						goto RESUB
					}
					callback([]byte(msg.Payload))
				case <-ctx.Done():
					_ = pubsub.Close()
					return
				}
			}
		RESUB:
			_ = pubsub.Close()
			time.Sleep(2 * time.Second)
		}
	}()
}

func (c *client) Emit(topic string, message []byte) error {
	return c.redisClient.XAdd(&redis.XAddArgs{
		Stream: topic,
		Values: map[string]interface{}{"msg": message},
		MaxLen: maxLen,
	}).Err()
}

func (c *client) ListenGroup(ctx context.Context, topic, group string, callback func(data []byte)) error {
	err := c.redisClient.XGroupCreateMkStream(topic, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}

	numCPU := runtime.NumCPU()
	workerCount := numCPU * 4
	var batchSize int64 = 1000
	var processed int64 = 0
	const maxIdle = 5 * time.Minute

	for w := 0; w < workerCount; w++ {
		consumerName := consumerPrefix + "_" + strconv.Itoa(w)

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				pending, _ := c.redisClient.XPendingExt(&redis.XPendingExtArgs{
					Stream: topic,
					Group:  group,
					Start:  "-",
					End:    "+",
					Count:  batchSize,
				}).Result()

				var idsToClaim []string
				for _, pend := range pending {
					if pend.Idle >= maxIdle {
						idsToClaim = append(idsToClaim, pend.Id)
					}
				}

				if len(idsToClaim) > 0 {
					res, _ := c.redisClient.XClaim(&redis.XClaimArgs{
						Stream:   topic,
						Group:    group,
						Consumer: consumerName,
						MinIdle:  0,
						Messages: idsToClaim,
					}).Result()

					var idsToAck []string
					for _, msg := range res {
						data := msg.Values["msg"].(string)
						go callback([]byte(data))
						idsToAck = append(idsToAck, msg.ID)
						atomic.AddInt64(&processed, 1)
					}
					if len(idsToAck) > 0 {
						c.redisClient.XAck(topic, group, idsToAck...)
						c.redisClient.XDel(topic, idsToAck...)
					}
				}

				res, err := c.redisClient.XReadGroup(&redis.XReadGroupArgs{
					Group:    group,
					Consumer: consumerName,
					Streams:  []string{topic, ">"},
					Count:    batchSize,
					Block:    0,
				}).Result()

				if err == nil {
					var idsToAck []string
					for _, stream := range res {
						for _, msg := range stream.Messages {
							data := msg.Values["msg"].(string)
							go callback([]byte(data))
							idsToAck = append(idsToAck, msg.ID)
							atomic.AddInt64(&processed, 1)
						}
					}
					if len(idsToAck) > 0 {
						c.redisClient.XAck(topic, group, idsToAck...)
						c.redisClient.XDel(topic, idsToAck...)
					}
				} else {
					time.Sleep(time.Second)
				}

				if atomic.LoadInt64(&processed) >= 10000 {
					atomic.StoreInt64(&processed, 0)
					streamLen, _ := c.redisClient.XLen(topic).Result()
					if streamLen > batchSize*2 {
						batchSize = int64(math.Min(float64(batchSize)*1.5, 10000))
					} else if streamLen < batchSize/2 {
						batchSize = int64(math.Max(float64(batchSize)/1.5, 500))
					}
				}
			}
		}()
	}

	return nil
}
