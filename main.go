package main

import (
	"fmt"
	"math/rand"

	"bitbucket.org/Soytul/library-go-redis/redis"
)

func main() {
	redisClient, err := redis.NewClient()
	if err != nil {
		fmt.Println("Error creating redis client: ", err)
		return
	}

	randomNumber := rand.Intn(100)
	randomValue := fmt.Sprintf("RandomValue-%d", randomNumber)

	redisClient.Set("key", []byte(randomValue), 0)

	result, err := redisClient.Get("key")
	if err != nil {
		fmt.Println("Error getting value: ", err)
		return
	}

	fmt.Println("Value: ", string(result))
}
