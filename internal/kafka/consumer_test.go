package kafka

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/douglasdgoulart/kafka-sql/internal/model"
	"github.com/douglasdgoulart/kafka-sql/internal/util"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaConsumer_Run(t *testing.T) {
	// Create a real Kafka client.
	opts := []kgo.Opt{kgo.SeedBrokers("localhost:9092")}
	cl, err := kgo.NewClient(opts...)
	require.NoError(t, err)

	// Create an Admin client.
	admin := kadm.NewClient(cl)

	// Create a topic.
	topic := "test_topic"
	_, err = admin.CreateTopics(context.Background(), 1, 1, nil, topic)
	require.NoError(t, err)

	_ = cl.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Value: []byte("test message")})

	// Create a KafkaConsumer with the real client and a message channel.
	msgChan := make(chan *model.Message)
	k := NewKafkaConsumer(&util.KafkaConfiguration{
		Brokers:         "localhost:9092",
		GroupID:         string(rand.Int31()),
		AutoOffsetReset: "earliest",
		Topic:           topic,
		SessionTimeout:  30000,
	}, msgChan)

	// Run the method in a separate goroutine.
	go k.Run(context.Background())

	// Wait for a message to be sent to the channel.
	select {
	case msg := <-msgChan:
		t.Logf("Received message: %v", *msg)
	case <-time.After(10 * time.Second):
		t.Fatal("No message received within timeout")
	}

	// Delete the topic.
	_, err = admin.DeleteTopics(context.Background(), topic)
	require.NoError(t, err)
}
