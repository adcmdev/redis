package main

import (
	"fmt"

	"bitbucket.org/Soytul/library-go-redis/redis"
)

func main() {
	redisClient, err := redis.NewClient("localhost:6379", "")
	if err != nil {
		fmt.Println("Error creating redis client: ", err)
		return
	}

	redisClient.Set("key", []byte("value"), 0)
}
