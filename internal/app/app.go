package app

import (
	"context"
	"fmt"
	"sync"

	"github.com/douglasdgoulart/kafka-sql/internal/db"
	"github.com/douglasdgoulart/kafka-sql/internal/kafka"
	"github.com/douglasdgoulart/kafka-sql/internal/model"
	"github.com/douglasdgoulart/kafka-sql/internal/util"
)

type App struct {
	msgChan    chan *model.Message
	config     *util.Configuration
	ctx        context.Context
	wg         *sync.WaitGroup
	repository *db.DBRepository
	k          *kafka.KafkaConsumer
}

func NewApp(config *util.Configuration, ctx context.Context) *App {
	msgChan := make(chan *model.Message)

	fmt.Printf("KafkaConfiguration: %+v\n", config.KafkaConfiguration)
	if ctx == nil {
		ctx = context.Background()
	}

	var wg sync.WaitGroup

	repository := db.NewDBRepository(nil)
	k := kafka.NewKafkaConsumer(config.KafkaConfiguration, msgChan)

	return &App{
		msgChan:    msgChan,
		config:     config,
		ctx:        ctx,
		wg:         &wg,
		repository: repository,
		k:          k,
	}
}

func (a *App) Run() {
	a.wg.Add(2)

	go a.runKafkaConsumer()
	go a.runDatabase()

	a.wg.Wait()
}

func (a *App) runKafkaConsumer() {
	defer func() {
		fmt.Println("Kafka consumer routine finished")
		a.wg.Done()
	}()
	a.k.Run(a.ctx)
}

func (a *App) runDatabase() {
	defer func() {
		fmt.Println("Database routine finished")
		a.wg.Done()
	}()
	for {
		select {
		case <-a.ctx.Done():
			return
		case msg := <-a.msgChan:
			err := a.repository.SaveMessage(msg)
			if err != nil {
				fmt.Printf("Error saving message: %s\n", err)
			}
		}
	}
}

func (a *App) Stop() {
	a.ctx.Done()
}
