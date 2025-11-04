package filesystemRegistry

import (
	"artifact-registry/config"
	"os"
	"path/filepath"
)

// GetStorageDir returns the storage directory based on config, ensuring it's absolute
func GetStorageDir() string {
	if !filepath.IsAbs(config.Cfg.StorageDir) {
		wd, _ := os.Getwd()
		return filepath.Join(wd, config.Cfg.StorageDir)
	}
	return config.Cfg.StorageDir
}