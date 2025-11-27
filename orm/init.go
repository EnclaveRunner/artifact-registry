package orm

import (
	"artifact-registry/config"
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
	"gorm.io/driver/postgres"

	"gorm.io/gorm/logger"

	"gorm.io/gorm"
)

type DB struct {
	dbGorm *gorm.DB
}

func InitDB(cfg *config.AppConfig) DB {
	dsn := fmt.Sprintf(
		"host='%s' port='%d' user='%s' password='%s' dbname='%s' sslmode='%s'",
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.Username,
		cfg.Database.Password,
		cfg.Database.Database,
		cfg.Database.SSLMode,
	)

	dsn_redacted := strings.ReplaceAll(dsn, cfg.Database.Password, "*****")
	log.Debug().
		Msgf("Connecting to postgres using the following information: %s", dsn_redacted)

	var err error
	dbGorm, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:         logger.Default.LogMode(logger.Silent),
		TranslateError: true,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to the database")
	}

	log.Debug().Msg("Successfully connected to the database")

	// Run database migrations
	err = dbGorm.AutoMigrate(&Artifact{}, &Tag{})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to migrate database")
	}

	return DB{dbGorm: dbGorm}
}

// UseTransaction returns a new DB instance that uses the provided gorm.DB
// transaction.
func (db *DB) UseTransaction(tx *gorm.DB) DB {
	// By only allowing transactions to be set via this method,
	// it is ensured that the function is called with an initialized db instance.
	return DB{dbGorm: tx}
}
