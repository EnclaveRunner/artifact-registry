package filesystemRegistry

import (
	"artifact-registry/config"
	"os"
	"path/filepath"
)

// GetStorageDir returns the storage directory based on config, ensuring it's absolute
func GetStorageDir() string {
	if config.Cfg.StorageDir == "" || config.Cfg.StorageDir[0] == '/' {
		wd, _ := os.Getwd()
		return filepath.Join(wd, config.Cfg.StorageDir)
	}

	return config.Cfg.StorageDir
}
