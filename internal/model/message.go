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
	Partition    int
	Offset       int
	Data         datatypes.JSON `gorm:"type:jsonb"`
}
