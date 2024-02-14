package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/douglasdgoulart/kafka-sql/internal/db"
	"github.com/douglasdgoulart/kafka-sql/internal/kafka"
	"github.com/douglasdgoulart/kafka-sql/internal/model"
	"github.com/douglasdgoulart/kafka-sql/internal/util"
)

func main() {
	msgChan := make(chan *model.Message)
	config := util.NewConfiguration()
	fmt.Printf("KafkaConfiguration: %+v\n", config.KafkaConfiguration)
	ctx := context.Background()

	var wg sync.WaitGroup

	repository := db.NewDBRepository(nil)
	k := kafka.NewKafkaConsumer(config.KafkaConfiguration, msgChan)

	wg.Add(1)
	go func() {
		defer wg.Done()
		k.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range msgChan {
			err := repository.SaveMessage(msg)
			if err != nil {
				fmt.Printf("Error saving message: %s\n", err)
			}
		}
	}()

	wg.Wait()
}
