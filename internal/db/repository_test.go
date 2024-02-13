package db

import (
	"math/rand"
	"testing"
	"time"

	"github.com/douglasdgoulart/kafka-sql/internal/model"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNewDBRepository(t *testing.T) {
	dialector := sqlite.Open("file::memory:?cache=shared")

	repo := NewDBRepository(dialector)
	require.NotNil(t, repo)
}

func TestDBRepository_SaveMessage(t *testing.T) {
	dialector := sqlite.Open("file::memory:?cache=shared")

	gdb, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)

	repo := NewDBRepository(dialector)
	repo.db = gdb

	message := &model.Message{
		InjetionTime: time.Now(),
		Topic:        "test_topic",
		Partition:    0,
		Offset:       0,
		Data:         datatypes.JSON([]byte(`{"test": "test"}`)),
	}
	message.ID = uint(rand.Uint32())

	repo.SaveMessage(message)

	var dbMessage *model.Message
	gdb.First(&dbMessage)
	require.Equal(t, message.ID, dbMessage.ID)
}

func TestDBRepository_SaveMessages(t *testing.T) {
	dialector := sqlite.Open("file::memory:?cache=shared")

	gdb, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)

	repo := NewDBRepository(dialector)
	repo.db = gdb

	messages := []*model.Message{
		{
			InjetionTime: time.Now(),
			Topic:        "test_topic",
			Partition:    0,
			Offset:       0,
			Data:         datatypes.JSON([]byte(`{"test": "test"}`)),
		},
		{
			InjetionTime: time.Now(),
			Topic:        "test_topic",
			Partition:    0,
			Offset:       1,
			Data:         datatypes.JSON([]byte(`{"test": "test"}`)),
		},
	}

	for _, msg := range messages {
		msg.ID = uint(rand.Uint32())
	}

	repo.SaveMessages(messages, nil)

	// Check that the messages were saved to the database
	var dbMessages []*model.Message
	gdb.Find(&dbMessages)

	var dbMessagesIds []uint
	for _, msg := range dbMessages {
		dbMessagesIds = append(dbMessagesIds, msg.ID)
	}

	for _, msg := range messages {
		require.Contains(t, dbMessagesIds, msg.ID)
	}

}
