package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	butil "github.com/douglasdgoulart/kafka-sql/benchmark/util"
	"github.com/douglasdgoulart/kafka-sql/internal/app"
	"github.com/douglasdgoulart/kafka-sql/internal/model"
	"github.com/douglasdgoulart/kafka-sql/internal/util"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func prepateTestTopic(ctx context.Context, topic string, topicMessagesCount int, messageBytesSize int) {
	fmt.Printf("Preparing test topic %s with %d messages of %d bytes size\n", topic, topicMessagesCount, messageBytesSize)
	opts := []kgo.Opt{kgo.SeedBrokers("localhost:9092")}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}

	admin := kadm.NewClient(cl)

	_, err = admin.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil {
		panic(err)
	}

	for counter := range topicMessagesCount {
		if (counter+1)%1000 == 0 {
			fmt.Printf("Producing message %d\n", counter+1)
		}

		message, _ := butil.GenerateRandomJSON(1000)
		_ = cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(message)})
	}
}

func main() {
	warmupMessages := 1000
	benchmarkMessages := 50000

	db, _ := gorm.Open(sqlite.Open("database.db"), &gorm.Config{})

	fmt.Println("Deleting data from db")
	db.Exec("DELETE FROM messages")
	fmt.Println("Data deleted")

	ctx, cancel := context.WithCancel(context.Background())
	topic_name := fmt.Sprintf("test-%d", rand.Int())
	group_id := fmt.Sprintf("test-group-%d", rand.Int())

	prepateTestTopic(ctx, topic_name, warmupMessages, 1000)

	config := &util.Configuration{
		KafkaConfiguration: &util.KafkaConfiguration{
			Brokers:            "localhost:9092",
			Topic:              topic_name,
			GroupID:            group_id,
			AutoOffsetReset:    "earliest",
			EnableAutoCommit:   true,
			AutoCommitInterval: 1000,
			SessionTimeout:     6000,
			HeartbeatInterval:  2000,
			MaxPollInterval:    5000,
		},
	}

	application := app.NewApp(config, ctx)

	wg := sync.WaitGroup{}

	wg.Add(2)
	go func() {
		defer func() {
			cancel()
			fmt.Println("Database warm up completed")
			wg.Done()
		}()

		t := time.NewTicker(1000 * time.Millisecond)
		for {
			select {
			case <-t.C:

				var dbMessages []*model.Message
				db.Find(&dbMessages)

				if len(dbMessages) >= warmupMessages {
					application.Stop()
					return
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		application.Run()
	}()

	wg.Wait()

	fmt.Println("Warmed up completed,")

	fmt.Println("Deleting data from db")
	db.Exec("DELETE FROM messages")
	fmt.Println("Data deleted")

	fmt.Println("Starting benchmark")

	ctx, cancel = context.WithCancel(context.Background())
	application = app.NewApp(config, ctx)
	prepateTestTopic(ctx, topic_name, benchmarkMessages, 1000)

	wg.Add(1)
	go func() {
		defer func() {
			cancel()
			fmt.Println("Database benchmark completed")
			wg.Done()
		}()
		db, _ := gorm.Open(sqlite.Open("database.db"), &gorm.Config{})
		t := time.NewTicker(1000 * time.Millisecond)
		for {
			select {
			case <-t.C:
				var dbMessages []*model.Message
				db.Find(&dbMessages)

				if len(dbMessages) >= benchmarkMessages {
					application.Stop()
					return
				}
			}
		}
	}()

	startTime := time.Now()
	wg.Add(1)
	go func() {
		defer wg.Done()
		application.Run()
	}()

	wg.Wait()
	stopTime := time.Now()

	fmt.Printf("Benchmark completed with %d messages in %s with a rate %fmsg/s\n", benchmarkMessages, stopTime.Sub(startTime), float64(benchmarkMessages)/stopTime.Sub(startTime).Seconds())
}
