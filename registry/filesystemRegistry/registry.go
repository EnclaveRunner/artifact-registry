package filesystemRegistry

import (
	"artifact-registry/proto_gen"
	"artifact-registry/registry"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

// ErrArtifactNotFound is returned when an artifact is not found
var ErrArtifactNotFound = errors.New("artifact not found")

// directory where artifacts are temporarily stored while they don't have a
// version hash
var uploadDir = "./uploads"

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
	reader io.Reader,
) (string, error) {
	uuidVal, err := uuid.NewUUID()
	if err != nil {
		return "", fmt.Errorf("failed to generate UUID: %w", err)
	}
	uniqueTempFileName := uploadDir + "/" + uuidVal.String() + ".tmp"

	// Ensure uniqueTempFileName is within the intended directory to prevent file
	// inclusion
	absUploadDir, err := filepath.Abs(uploadDir)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute upload directory: %w", err)
	}
	absTempFileName, err := filepath.Abs(uniqueTempFileName)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute temp file name: %w", err)
	}
	absUploadDirClean := filepath.Clean(absUploadDir) + string(os.PathSeparator)
	absTempFileNameClean := filepath.Clean(absTempFileName)
	if len(absTempFileNameClean) < len(absUploadDirClean) ||
		absTempFileNameClean[:len(absUploadDirClean)] != absUploadDirClean {
		return "", fmt.Errorf("%w: %s", ErrArtifactNotFound, absTempFileName)
	}

	// Ensure the uploads directory exists
	//nolint:gosec,mnd // Directory permissions 0755 are intentional
	if err := os.MkdirAll(absUploadDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create upload directory: %w", err)
	}

	// Create or open a file for writing
	file, err := os.Create(absTempFileNameClean)
	if err != nil {
		return "", fmt.Errorf("error creating file: %w", err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			err = fmt.Errorf("error closing file: %w", cerr)
		}
	}()

	// Create a hash.Hash to compute the checksum
	h := sha256.New()

	// Create a multi-writer to write to both the file and the hash
	multiWriter := io.MultiWriter(file, h)

	// Buffer to read chunks into
	buf := make([]byte, registry.ChunkSize)

	for {
		// Read into buf from the PipeReader
		n, err := reader.Read(buf)
		if err == io.EOF {
			break // end of stream
		}
		if err != nil {
			return "", fmt.Errorf("error reading chunk: %w", err)
		}

		// Write the buffer to the multi-writer
		if _, err := multiWriter.Write(buf[:n]); err != nil {
			return "", fmt.Errorf("error writing to multi-writer: %w", err)
		}
	}

	// Generate version hash
	versionHash := hex.EncodeToString(h.Sum(nil))

	// Rename the temp file to the final path
	finalPath := r.getArtifactPath(fqn, versionHash)
	// Ensure the final directory exists before renaming
	finalDir := filepath.Dir(finalPath)

	//nolint:gosec,mnd // Directory permissions 0755 are intentional
	if err := os.MkdirAll(finalDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create final directory: %w", err)
	}
	if err := os.Rename(absTempFileNameClean, finalPath); err != nil {
		return "", fmt.Errorf("failed to rename temp file: %w", err)
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
