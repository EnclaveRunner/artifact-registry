package s3

import (
	"artifact-registry/config"
	"artifact-registry/proto_gen"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog/log"
)

// ErrArtifactNotFound is returned when an artifact is not found
var ErrArtifactNotFound = errors.New("artifact not found")

// S3Registry implements the registry interface using an s3-backed
// storage
type S3Registry struct {
	S3Client *s3.Client
}

// New creates a new s3-based registry
func New() (*S3Registry, error) {
	// check for required S3 configuration
	if config.Cfg.Persistence.S3.AccessKey == "" ||
		config.Cfg.Persistence.S3.KeyID == "" ||
		config.Cfg.Persistence.S3.Endpoint == "" ||
		config.Cfg.Persistence.S3.Region == "" {
		return nil, fmt.Errorf("incomplete S3 configuration")
	}
	s3Client := s3.New(s3.Options{
        BaseEndpoint: aws.String(config.Cfg.Persistence.S3.Endpoint),
        Region: config.Cfg.Persistence.S3.Region,
        Credentials: aws.NewCredentialsCache(
            credentials.NewStaticCredentialsProvider(
                config.Cfg.Persistence.S3.KeyID,
                config.Cfg.Persistence.S3.AccessKey,
                "",
            ),
        ),
    })

	return &S3Registry{S3Client: s3Client}, nil
}

// StoreArtifact stores an artifact in the bucket and returns its version
// hash
func (r *S3Registry) StoreArtifact(
	fqn *proto_gen.FullQualifiedName,
	content []byte,
) (string, error) {
	// Generate version hash if not provided
	hash := sha256.Sum256(content)
	versionHash := hex.EncodeToString(hash[:])

	// Create directory structure
	artifactPath := r.getArtifactPath(fqn, versionHash)

	uploader := manager.NewUploader(r.S3Client)
	result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(config.Cfg.Persistence.S3.Bucket),
		Key:    aws.String(artifactPath),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		var mu manager.MultiUploadFailure
		if errors.As(err, &mu) {
			// Process error and its associated uploadID
			log.Error().Msg("Error: " + mu.Error() + " id: " + mu.UploadID())
		} else {
			// Process error generically
			log.Error().Msg("Error:" + err.Error())
		}
	}
	log.Printf("Successfully uploaded artifact to s3 bucket (%s)\n", result.Location)

	return versionHash, nil
}

// GetArtifact retrieves an artifact by identifier
func (r *S3Registry) GetArtifact(
	fqn *proto_gen.FullQualifiedName,
	hash string,
) ([]byte, error) {
	artifactPath := r.getArtifactPath(fqn, hash)

	object, err := r.S3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(config.Cfg.Persistence.S3.Bucket),
		Key:    aws.String(artifactPath),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get artifact from S3: %w", err)
	}

	var content []byte
	if object.Body != nil {
		defer object.Body.Close()
		content, err = io.ReadAll(object.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read artifact content: %w", err)
		}
	} else {
		content = []byte{}
	}

	return content, nil
}

// DeleteArtifact deletes an artifact by identifier
func (r *S3Registry) DeleteArtifact(
	fqn *proto_gen.FullQualifiedName,
	hash string,
) error {
	artifactPath := r.getArtifactPath(fqn, hash)

	_, err := r.S3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(config.Cfg.Persistence.S3.Bucket),
		Key:    aws.String(artifactPath),
	})

	if err != nil {
		return fmt.Errorf("failed to delete artifact from S3: %w", err)
	}

	return nil
}

// getArtifactPath returns the file path / object key for an artifact
func (r *S3Registry) getArtifactPath(
	fqn *proto_gen.FullQualifiedName,
	versionHash string,
) string {
	return filepath.Join(
		fqn.Source,
		fqn.Author,
		fqn.Name,
		versionHash+".wasm",
	)
}
