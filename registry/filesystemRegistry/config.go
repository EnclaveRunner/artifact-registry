package filesystemRegistry

import (
	"artifact-registry/config"
	"os"
	"path/filepath"
)

// GetStorageDir returns the storage directory based on config, ensuring it's
// absolute
func GetStorageDir(cfg *config.AppConfig) string {
	if cfg.StorageDir == "" || cfg.StorageDir[0] == '/' {
		wd, _ := os.Getwd()

		return filepath.Join(wd, cfg.StorageDir)
	}

	return cfg.StorageDir
}
