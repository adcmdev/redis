package redis

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"runtime"
	"strconv"
	"strings"
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

func streamForKey(prefix string, key string, shards int) string {
	if shards <= 1 || key == "" {
		return prefix
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	idx := int(h.Sum32()) % shards
	return fmt.Sprintf("%s:%d", prefix, idx)
}

func (c *client) Emit(topicPrefix, partitionKey string, message []byte, shards int) error {
	stream := streamForKey(topicPrefix, partitionKey, shards)

	return c.redisClient.XAdd(&redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"msg": message},
	}).Err()
}

func (c *client) ListenGroup(ctx context.Context, topic, group string, callback func(data [][]byte)) error {
	err := c.redisClient.XGroupCreateMkStream(topic, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}

	numCPU := runtime.NumCPU()
	workerCount := numCPU * 2

	const (
		minBatchSize int64         = 100
		maxBatchSize int64         = 500
		maxIdle      time.Duration = 5 * time.Minute
		blockTime    time.Duration = 2 * time.Second
	)

	batchSize := minBatchSize

	for w := 0; w < workerCount; w++ {
		consumerName := consumerPrefix + "_" + strconv.Itoa(w)

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				for {
					pending, err := c.redisClient.XPendingExt(&redis.XPendingExtArgs{
						Stream: topic,
						Group:  group,
						Start:  "-",
						End:    "+",
						Count:  batchSize,
					}).Result()
					if err != nil || len(pending) == 0 {
						break
					}

					var idsToClaim []string
					for _, msg := range pending {
						if msg.Idle >= maxIdle {
							idsToClaim = append(idsToClaim, msg.Id)
						}
					}

					if len(idsToClaim) == 0 {
						break
					}

					res, err := c.redisClient.XClaim(&redis.XClaimArgs{
						Stream:   topic,
						Group:    group,
						Consumer: consumerName,
						MinIdle:  0,
						Messages: idsToClaim,
					}).Result()
					if err != nil || len(res) == 0 {
						break
					}

					var dataBatch [][]byte
					var ids []string
					for _, msg := range res {
						raw := msg.Values["msg"].(string)
						dataBatch = append(dataBatch, []byte(raw))
						ids = append(ids, msg.ID)
					}

					callback(dataBatch)
					c.redisClient.XAck(topic, group, ids...)
					c.redisClient.XDel(topic, ids...)
				}

				streams, err := c.redisClient.XReadGroup(&redis.XReadGroupArgs{
					Group:    group,
					Consumer: consumerName,
					Streams:  []string{topic, ">"},
					Count:    batchSize,
					Block:    blockTime,
				}).Result()

				if err == redis.Nil {
					continue
				} else if err != nil {
					log.Printf("XReadGroup error: %v", err)
					time.Sleep(time.Second)
					continue
				}

				for _, s := range streams {
					if len(s.Messages) == 0 {
						continue
					}

					var dataBatch [][]byte
					var ids []string
					for _, msg := range s.Messages {
						raw := msg.Values["msg"].(string)
						dataBatch = append(dataBatch, []byte(raw))
						ids = append(ids, msg.ID)
					}

					callback(dataBatch)
					c.redisClient.XAck(topic, group, ids...)
					c.redisClient.XDel(topic, ids...)
				}

				streamLen, _ := c.redisClient.XLen(topic).Result()
				if streamLen > batchSize*2 {
					batchSize = int64(math.Min(float64(batchSize)*1.2, float64(maxBatchSize)))
				} else if streamLen < batchSize/2 {
					batchSize = int64(math.Max(float64(batchSize)/1.2, float64(minBatchSize)))
				}
			}
		}()
	}

	return nil
}

func (c *client) ListenGroupSharded(ctx context.Context, topicPrefix, group string, shards int, callback func(data [][]byte)) error {
	for shard := 0; shard < shards; shard++ {
		stream := fmt.Sprintf("%s:%d", topicPrefix, shard)

		if err := c.ListenGroup(ctx, stream, group, callback); err != nil {
			return err
		}
	}

	return nil
}
