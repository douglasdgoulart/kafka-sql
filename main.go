package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/douglasdgoulart/kafka-sql/internal/kafka"
	"github.com/douglasdgoulart/kafka-sql/internal/util"
)

func main() {
	msgChan := make(chan *[]byte)
	config := util.NewConfiguration()
	fmt.Printf("KafkaConfiguration: %+v\n", config.KafkaConfiguration)
	ctx := context.Background()

	var wg sync.WaitGroup

	k := kafka.NewKafkaConsumer(config.KafkaConfiguration, msgChan)

	wg.Add(1)
	go func() {
		defer wg.Done()
		k.Run(ctx)
	}()

	for msg := range msgChan {
		fmt.Printf("Received message: %s\n", *msg)
	}

	fmt.Println("Hello, World!")
}
