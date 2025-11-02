package filesystemRegistry

import (
	"artifact-registry/proto_gen"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// FilesystemRegistry implements the registry interface using simple filesystem storage
type FilesystemRegistry struct {
	baseDir string
}

// New creates a new filesystem-based registry
func New(baseDir string) (*FilesystemRegistry, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}
	return &FilesystemRegistry{baseDir: baseDir}, nil
}

// getArtifactPath returns the file path for an artifact
func (r *FilesystemRegistry) getArtifactPath(fqn *proto_gen.FullQualifiedName, versionHash string) string {
	return filepath.Join(r.baseDir, fqn.Source, fqn.Author, fqn.Name, versionHash+".json")
}

// StoreArtifact stores an artifact in the filesystem
func (r *FilesystemRegistry) StoreArtifact(artifact *proto_gen.Artifact) error {
	// Generate version hash if not provided
	if artifact.VersionHash == "" {
		hash := sha256.Sum256(artifact.Content)
		artifact.VersionHash = hex.EncodeToString(hash[:])
	}

	// Set creation timestamp if not provided
	if artifact.Metadata == nil {
		artifact.Metadata = &proto_gen.MetaData{
			Created: timestamppb.New(time.Now()),
		}
	}

	// Create directory structure
	artifactPath := r.getArtifactPath(artifact.Fqn, artifact.VersionHash)
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write artifact to file
	data, err := json.Marshal(artifact)
	if err != nil {
		return fmt.Errorf("failed to marshal artifact: %w", err)
	}

	return os.WriteFile(artifactPath, data, 0644)
}

// GetArtifact retrieves an artifact by identifier
func (r *FilesystemRegistry) GetArtifact(id *proto_gen.ArtifactIdentifier) (*proto_gen.Artifact, error) {
	var versionHash string

	switch identifier := id.Identifier.(type) {
	case *proto_gen.ArtifactIdentifier_VersionHash:
		versionHash = identifier.VersionHash
	case *proto_gen.ArtifactIdentifier_Tag:
		// TODO: Implement tag lookup
		return nil, fmt.Errorf("tag lookup not implemented yet")
	default:
		return nil, fmt.Errorf("invalid identifier type")
	}

	artifactPath := r.getArtifactPath(id.Fqn, versionHash)
	data, err := os.ReadFile(artifactPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("artifact not found")
		}
		return nil, fmt.Errorf("failed to read artifact: %w", err)
	}

	var artifact proto_gen.Artifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		return nil, fmt.Errorf("failed to unmarshal artifact: %w", err)
	}

	return &artifact, nil
}

// DeleteArtifact deletes an artifact by identifier
func (r *FilesystemRegistry) DeleteArtifact(id *proto_gen.ArtifactIdentifier) (*proto_gen.Artifact, error) {
	// First get the artifact to return it
	artifact, err := r.GetArtifact(id)
	if err != nil {
		return nil, err
	}

	// Remove the file
	artifactPath := r.getArtifactPath(id.Fqn, artifact.VersionHash)
	if err := os.Remove(artifactPath); err != nil {
		return nil, fmt.Errorf("failed to remove artifact: %w", err)
	}

	return artifact, nil
}

// QueryArtifacts searches for artifacts TODO: unimplemented
func (r *FilesystemRegistry) QueryArtifacts(query *proto_gen.ArtifactQuery) ([]*proto_gen.Artifact, error) {
	// unimplemented
	return []*proto_gen.Artifact{}, nil
}
