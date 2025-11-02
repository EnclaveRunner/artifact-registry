package filesystemRegistry

import (
	"os"
	"path/filepath"
)

// GetStorageDir returns the default storage directory
func GetStorageDir() string {
	homeDir, _ := os.UserHomeDir()
	return filepath.Join(homeDir, ".artifact-registry")
}
