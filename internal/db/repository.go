package db

import (
	"github.com/douglasdgoulart/kafka-sql/internal/model"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type DBRepository struct {
	db *gorm.DB
}

func NewDBRepository(dialector gorm.Dialector) *DBRepository {
	if dialector == nil {
		dialector = sqlite.Open("database.db")
	}
	db, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	db.AutoMigrate(&model.Message{})

	return &DBRepository{
		db: db,
	}
}

func (d *DBRepository) SaveMessage(message *model.Message) error {
	tx := d.db.Clauses(clause.OnConflict{DoNothing: true}).Create(message)
	return tx.Error
}

func (d *DBRepository) SaveMessages(messages []*model.Message, batchSize *int) {
	if batchSize == nil {
		defaultBatchSize := 1000
		batchSize = &defaultBatchSize
	}
	d.db.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(messages, *batchSize)
}
