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
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/rs/zerolog/log"
)

// ErrIncompleteS3Config is returned when the S3 configuration is incomplete
var ErrIncompleteS3Config = errors.New("incomplete S3 configuration")

// ErrArtifactNotFound is returned when an artifact is not found
var ErrArtifactNotFound = errors.New("artifact not found")

// S3Registry implements the registry interface using an s3-backed
// storage
type S3Registry struct {
	S3Client *s3.Client
	Timeout  time.Duration
	Bucket   string
}

// New creates a new s3-based registry
func New() (*S3Registry, error) {
	// check for required S3 configuration
	if strings.TrimSpace(config.Cfg.Persistence.S3.AccessKey) == "" ||
		strings.TrimSpace(config.Cfg.Persistence.S3.KeyID) == "" ||
		strings.TrimSpace(config.Cfg.Persistence.S3.Endpoint) == "" ||
		strings.TrimSpace(config.Cfg.Persistence.S3.Region) == "" ||
		strings.TrimSpace(config.Cfg.Persistence.S3.Bucket) == "" ||
		strings.TrimSpace(config.Cfg.Persistence.S3.Timeout) == "" {
		return nil, fmt.Errorf("%w", ErrIncompleteS3Config)
	}
	s3Client := s3.New(s3.Options{
		UsePathStyle: true,
		BaseEndpoint: aws.String(config.Cfg.Persistence.S3.Endpoint),
		Region:       config.Cfg.Persistence.S3.Region,
		Credentials: aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(
				config.Cfg.Persistence.S3.KeyID,
				config.Cfg.Persistence.S3.AccessKey,
				"",
			),
		),
	})

	timeoutDuration, err := time.ParseDuration(config.Cfg.Persistence.S3.Timeout)
	if err != nil {
		return nil, fmt.Errorf("invalid S3 timeout value: %w", err)
	}

	return &S3Registry{
		S3Client: s3Client,
		Timeout:  timeoutDuration,
		Bucket:   config.Cfg.Persistence.S3.Bucket,
	}, nil
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

	// Create directory structure and upload artifact
	artifactPath := r.getArtifactPath(fqn, versionHash)

	uploader := manager.NewUploader(r.S3Client)

	ctx, cancel := context.WithTimeout(context.Background(), r.Timeout)
	defer cancel()
	result, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.Bucket),
		Key:    aws.String(artifactPath),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		var mu manager.MultiUploadFailure
		if errors.As(err, &mu) {
			// Process error and its associated uploadID
			log.Error().
				Msg(fmt.Sprintf("multi-upload failure (upload_id: %s): %v", mu.UploadID(), mu))

			return "", fmt.Errorf(
				"multi-upload failure (upload_id: %s): %w",
				mu.UploadID(),
				mu,
			)
		} else {
			// Process error generically
			log.Error().Err(err).Msg("upload failure")

			return "", fmt.Errorf("upload failure: %w", err)
		}
	}
	log.Info().
		Str("location", result.Location).
		Msg("successfully uploaded artifact to s3 bucket")

	return versionHash, nil
}

// GetArtifact retrieves an artifact by identifier
func (r *S3Registry) GetArtifact(
	fqn *proto_gen.FullQualifiedName,
	hash string,
) ([]byte, error) {
	artifactPath := r.getArtifactPath(fqn, hash)

	ctx, cancel := context.WithTimeout(context.Background(), r.Timeout)
	defer cancel()
	object, err := r.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.Bucket),
		Key:    aws.String(artifactPath),
	})
	if err != nil {
		var notFoundErr *types.NotFound
		if errors.As(err, &notFoundErr) {
			return nil, ErrArtifactNotFound
		}

		return nil, fmt.Errorf("failed to get artifact from S3: %w", err)
	}

	var content []byte
	if object.Body != nil {
		defer func() {
			if cerr := object.Body.Close(); cerr != nil {
				log.Error().Err(cerr).Msg("failed to close S3 object body")
			}
		}()
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

	// check if object exists before attempting deletion
	content, err := r.GetArtifact(fqn, hash)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrArtifactNotFound, err)
	}
	if len(content) == 0 {
		return fmt.Errorf(
			"%w: artifact is empty, cannot delete",
			ErrArtifactNotFound,
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.Timeout)
	defer cancel()
	_, err = r.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(r.Bucket),
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
	return path.Join(
		fqn.Source,
		fqn.Author,
		fqn.Name,
		versionHash+".wasm",
	)
}
