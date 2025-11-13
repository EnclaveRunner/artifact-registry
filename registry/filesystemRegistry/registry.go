package filesystemRegistry

import (
	"artifact-registry/proto_gen"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrArtifactNotFound is returned when an artifact is not found
var ErrArtifactNotFound = errors.New("artifact not found")

// FilesystemRegistry implements the registry interface using simple filesystem
// storage
type FilesystemRegistry struct {
	baseDir string
}

// New creates a new filesystem-based registry
func New(baseDir string) (*FilesystemRegistry, error) {
	//nolint:gosec,mnd // Directory permissions 0755 are intentional
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &FilesystemRegistry{baseDir: baseDir}, nil
}

// StoreArtifact stores an artifact in the filesystem and returns its version
// hash
func (r *FilesystemRegistry) StoreArtifact(
	fqn *proto_gen.FullyQualifiedName,
	content []byte,
) (string, error) {
	// Generate version hash if not provided
	hash := sha256.Sum256(content)
	versionHash := hex.EncodeToString(hash[:])

	// Create directory structure
	artifactPath := r.getArtifactPath(fqn, versionHash)

	//nolint:gosec,mnd // Directory permissions 0755 are intentional
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0o755); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}
	//nolint:mnd // filemode constant
	if err := os.WriteFile(artifactPath, content, 0o644); err != nil {
		return versionHash, fmt.Errorf("failed to write file: %w", err)
	}

	return versionHash, nil
}

// GetArtifact retrieves an artifact by identifier
func (r *FilesystemRegistry) GetArtifact(
	fqn *proto_gen.FullyQualifiedName,
	hash string,
) ([]byte, error) {
	artifactPath := r.getArtifactPath(fqn, hash)
	//nolint:gosec // G304: File path is constructed internally and validated
	content, err := os.ReadFile(artifactPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrArtifactNotFound
		}

		return nil, fmt.Errorf("failed to read artifact: %w", err)
	}

	return content, nil
}

// DeleteArtifact deletes an artifact by identifier
func (r *FilesystemRegistry) DeleteArtifact(
	fqn *proto_gen.FullyQualifiedName,
	hash string,
) error {
	// Remove the file
	artifactPath := r.getArtifactPath(fqn, hash)
	if err := os.Remove(artifactPath); err != nil {
		return fmt.Errorf("failed to remove artifact: %w", err)
	}

	return nil
}

// getArtifactPath returns the file path for an artifact
func (r *FilesystemRegistry) getArtifactPath(
	fqn *proto_gen.FullyQualifiedName,
	versionHash string,
) string {
	return filepath.Join(
		r.baseDir,
		fqn.Source,
		fqn.Author,
		fqn.Name,
		versionHash+".wasm",
	)
}
