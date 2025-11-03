package filesystemRegistry

import (
	"artifact-registry/orm"
	"artifact-registry/proto_gen"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gorm.io/gorm"
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

// StoreArtifact stores an artifact in the filesystem and its metadata in the database
func (r *FilesystemRegistry) StoreArtifact(fqn *proto_gen.FullQualifiedName, content []byte) (string, error) {
	// Generate version hash if not provided
	hash := sha256.Sum256(content)
	versionHash := hex.EncodeToString(hash[:])
	
	// store metadata in database
	orm.DB.Save(
		&orm.Artifact{
			Source:   fqn.Source,
			Author:  fqn.Author,
			Name:    fqn.Name,
		},
	)

	// Create directory structure
	artifactPath := r.getArtifactPath(fqn, versionHash)
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0755); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}

	// Write artifact to file
	data, err := json.Marshal(content)
	if err != nil {
		return "", fmt.Errorf("failed to marshal artifact: %w", err)
	}

	return versionHash, os.WriteFile(artifactPath, data, 0644)
}

// GetArtifact retrieves an artifact by identifier
func (r *FilesystemRegistry) GetArtifact(id *proto_gen.ArtifactIdentifier) (*proto_gen.Artifact, error) {
	var versionHash string
	var query orm.Tag

	query = orm.Tag{
			Source: id.Fqn.Source,
			Author: id.Fqn.Author,
			Name:   id.Fqn.Name,
			}

	switch identifier := id.Identifier.(type) {
	case *proto_gen.ArtifactIdentifier_VersionHash:
		query.Hash = identifier.VersionHash
	case *proto_gen.ArtifactIdentifier_Tag:
		query.TagName = identifier.Tag
	default:
		return nil, fmt.Errorf("invalid identifier type")
	}

	tagRecord, err := gorm.G[orm.Tag](
			orm.DB,
	).Where(&query).Find(context.Background())
		
	if err != nil {
		return nil, gorm.ErrRecordNotFound
	}
	// if a fqn + tag combination is not unique, return the first match
	versionHash = tagRecord[0].Hash
	tags := []string{}
	for _, record := range tagRecord {
		tags = append(tags, record.TagName)
	}

	artifactPath := r.getArtifactPath(id.Fqn, versionHash)
	data, err := os.ReadFile(artifactPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("artifact not found")
		}
		return nil, fmt.Errorf("failed to read artifact: %w", err)
	}

	var content []byte
	if err := json.Unmarshal(data, &content); err != nil {
		return nil, fmt.Errorf("failed to unmarshal artifact: %w", err)
	}

	return &proto_gen.Artifact{
		Fqn:         id.Fqn,
		VersionHash: versionHash,
		Tags:         tags,
		Content:     content,
	}, nil
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

	// cleanup database records
	orm.DB.Delete(
		&orm.Artifact{
			Source:   id.Fqn.Source,
			Author: id.Fqn.Author,
			Name:   id.Fqn.Name,
			Hash:  artifact.VersionHash,
		},
	)

	orm.DB.Delete(
		&orm.Tag{
			Source:   id.Fqn.Source,
			Author: id.Fqn.Author,
			Name:   id.Fqn.Name,
			Hash:  artifact.VersionHash,
		},
	)
	return artifact, nil
}

// QueryArtifacts searches for artifacts
func (r *FilesystemRegistry) QueryArtifacts(query *proto_gen.ArtifactQuery) ([]*proto_gen.Artifact, error) {
	var dbQuery orm.Artifact

	if query.Source != nil {
		dbQuery.Source = *query.Source
	}
	if query.Author != nil {
		dbQuery.Author = *query.Author
	}
	if query.Name != nil {
		dbQuery.Name = *query.Name
	}
	artifactRecords, err := gorm.G[orm.Artifact](
			orm.DB,
	).Where(&dbQuery).Find(context.Background())
	if err != nil {
		return nil, err
	}

	artifacts := make([]*proto_gen.Artifact, 0, len(artifactRecords))
	for _, record := range artifactRecords {
		artifact, err := r.GetArtifact(&proto_gen.ArtifactIdentifier{
			Fqn: &proto_gen.FullQualifiedName{
				Source: record.Source,
				Author: record.Author,
				Name:   record.Name,
			},
			Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
				VersionHash: record.Hash,
			},
		})
		if err == nil {
			artifacts = append(artifacts, artifact)
		}
	}
	return artifacts, nil
}
