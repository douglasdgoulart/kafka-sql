package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/douglasdgoulart/kafka-sql/internal/util"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaConsumer struct {
	client  *kgo.Client
	msgChan chan<- *[]byte
}

func NewKafkaConsumer(config *util.KafkaConfiguration, msgChan chan<- *[]byte) *KafkaConsumer {
	brokers := strings.Split(config.Brokers, ",")
	topics := strings.Split(config.Topic, ",")
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(config.GroupID),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(time.Duration(config.SessionTimeout*int(time.Millisecond))),
		// TODO: add the missing configurations
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Trying to ping kafka")
	err = cl.Ping(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("Kafka consumer created")

	return &KafkaConsumer{
		client:  cl,
		msgChan: msgChan,
	}
}

func (k *KafkaConsumer) Run(ctx context.Context) {
	defer k.client.Close()
	for {
		fetches := k.client.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			panic(fmt.Sprint(errs))
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			go func() {
				k.msgChan <- &record.Value
			}()
		}
	}
}
