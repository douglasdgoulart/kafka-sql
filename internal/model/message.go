package model

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type Message struct {
	gorm.Model
	InjetionTime time.Time
	Topic        string
	Partition    int32
	Offset       int64
	Data         datatypes.JSON `gorm:"type:jsonb"`
}
