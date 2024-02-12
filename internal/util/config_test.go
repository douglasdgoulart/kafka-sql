package util

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConfiguration(t *testing.T) {
	// Call the function to test
	config := NewConfiguration()

	// Use testify's assert package for more readable assertions
	assert := assert.New(t)

	// Check the values
	assert.Equal("localhost:9092", config.KafkaConfiguration.Brokers, "Expected kafka_brokers to be localhost:9092")
	assert.Equal("test_topic", config.KafkaConfiguration.Topic, "Expected kafka_topic to be test_topic")

	// Add more checks for the other fields as needed...

	// Clean up environment variables after testing
	os.Clearenv()
}
