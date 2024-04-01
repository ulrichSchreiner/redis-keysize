package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

const (
	pipelineSize = 500
)

var (
	ctx      = context.Background()
	address  = flag.String("address", "localhost:6379", "the address of the redis server")
	password = flag.String("password", "", "the password for the redis connection")
	redisdb  = flag.Int("database", 0, "the redis database to use")
)

func main() {
	flag.Parse()
	rdb := redis.NewClient(&redis.Options{
		Addr:     *address,
		Password: *password,
		DB:       *redisdb,
	})

	var cursor uint64
	var allkeys []string
	for {
		var keys []string
		var err error
		keys, cursor, err = rdb.Scan(ctx, cursor, "*", 100).Result()
		if err != nil {
			panic(err)
		}
		allkeys = append(allkeys, keys...)
		if cursor == 0 {
			break
		}
	}

	csvout := csv.NewWriter(os.Stdout)
	pipe := rdb.Pipeline()

	for i, key := range allkeys {
		_ = pipe.MemoryUsage(ctx, key, 0)
		if i&pipelineSize == 0 || i == len(allkeys)-1 {
			sizes, err := pipe.Exec(ctx)
			if err != nil {
				if !errors.Is(err, redis.Nil) {
					panic(err)
				}
			}
			for _, size := range sizes {
				isz := size.(*redis.IntCmd)
				val := isz.Val()
				key := size.Args()[2].(string)
				_ = csvout.Write([]string{fmt.Sprintf("%d", val), key})
			}
			pipe = rdb.Pipeline()
		}
	}
	csvout.Flush()
}
