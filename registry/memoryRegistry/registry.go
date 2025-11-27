package memoryRegistry

import (
	"artifact-registry/proto_gen"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrArtifactNotFound is returned when an artifact is not found
var ErrArtifactNotFound = errors.New("artifact not found")

// MemoryRegistry implements the registry interface using in-memory storage.
// Used only for testing.
type MemoryRegistry struct {
	mu        sync.RWMutex
	artifacts map[string][]byte
}

// New creates a new memory-based registry
func New() *MemoryRegistry {
	return &MemoryRegistry{
		artifacts: make(map[string][]byte),
	}
}

// StoreArtifact stores an artifact in memory and returns its version hash
func (r *MemoryRegistry) StoreArtifact(
	fqn *proto_gen.FullyQualifiedName,
	reader io.Reader,
) (string, error) {
	// Read all content from reader
	content, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("failed to read artifact content: %w", err)
	}

	// Compute SHA256 hash
	h := sha256.New()
	h.Write(content)
	versionHash := hex.EncodeToString(h.Sum(nil))

	// Generate storage key
	key := r.getArtifactKey(fqn, versionHash)

	// Store in memory
	r.mu.Lock()
	r.artifacts[key] = content
	r.mu.Unlock()

	return versionHash, nil
}

// GetArtifact retrieves an artifact by identifier
func (r *MemoryRegistry) GetArtifact(
	fqn *proto_gen.FullyQualifiedName,
	hash string,
) ([]byte, error) {
	key := r.getArtifactKey(fqn, hash)

	r.mu.RLock()
	content, exists := r.artifacts[key]
	r.mu.RUnlock()

	if !exists {
		return nil, ErrArtifactNotFound
	}

	// Return a copy to prevent external modifications
	result := make([]byte, len(content))
	copy(result, content)

	return result, nil
}

// DeleteArtifact deletes an artifact by identifier
func (r *MemoryRegistry) DeleteArtifact(
	fqn *proto_gen.FullyQualifiedName,
	hash string,
) error {
	key := r.getArtifactKey(fqn, hash)

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.artifacts[key]; !exists {
		return fmt.Errorf("failed to remove artifact: artifact not found")
	}

	delete(r.artifacts, key)

	return nil
}

// getArtifactKey returns the storage key for an artifact
func (r *MemoryRegistry) getArtifactKey(
	fqn *proto_gen.FullyQualifiedName,
	versionHash string,
) string {
	return fmt.Sprintf(
		"%s/%s/%s/%s",
		fqn.Source,
		fqn.Author,
		fqn.Name,
		versionHash,
	)
}

// Clear removes all artifacts from memory (useful for testing)
func (r *MemoryRegistry) Clear() {
	r.mu.Lock()
	r.artifacts = make(map[string][]byte)
	r.mu.Unlock()
}

// Count returns the number of artifacts stored (useful for testing)
func (r *MemoryRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.artifacts)
}
