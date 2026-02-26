package filesystemRegistry

import (
	"artifact-registry/proto_gen"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

type IOError struct {
	Operation string
	Err       error
}

func (e *IOError) Error() string {
	return "I/O error during " + e.Operation + ": " + e.Err.Error()
}

func (e *IOError) Unwrap() error {
	return e.Err
}

var ErrIllegalPath = errors.New(
	"provided FQN results in an illegal filepath that lays outside the upload directory",
)

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
		return nil, &IOError{
			"creating base directory",
			err,
		}
	}

	return &FilesystemRegistry{baseDir: baseDir}, nil
}

// StoreArtifact stores an artifact in the filesystem and returns its version
// hash
func (r *FilesystemRegistry) StoreArtifact(
	pkg *proto_gen.PackageName,
	reader io.Reader,
) (string, error) {
	uuidVal, err := uuid.NewUUID()
	if err != nil {
		return "", &IOError{
			"generating temp file name",
			err,
		}
	}
	uniqueTempFileName := filepath.Join(uploadDir, uuidVal.String()+".tmp")

	// Ensure uniqueTempFileName is within the intended directory to prevent file
	// inclusion
	absUploadDir, err := filepath.Abs(uploadDir)
	if err != nil {
		return "", &IOError{
			"parsing upload directory path",
			err,
		}
	}
	absTempFileName, err := filepath.Abs(uniqueTempFileName)
	if err != nil {
		return "", &IOError{
			"parsing temp file path",
			err,
		}
	}
	absUploadDirClean := filepath.Clean(absUploadDir) + string(os.PathSeparator)
	absTempFileNameClean := filepath.Clean(absTempFileName)
	if len(absTempFileNameClean) < len(absUploadDirClean) ||
		absTempFileNameClean[:len(absUploadDirClean)] != absUploadDirClean {
		return "", &IOError{
			"parsing destination path",
			ErrIllegalPath,
		}
	}

	// Ensure the uploads directory exists
	//nolint:gosec,mnd // Directory permissions 0755 are intentional
	if err := os.MkdirAll(absUploadDir, 0o755); err != nil {
		return "", &IOError{
			"creating upload directory",
			err,
		}
	}

	// Create or open a file for writing
	file, err := os.Create(absTempFileNameClean)
	if err != nil {
		return "", &IOError{
			"creating artifact temp file",
			err,
		}
	}
	defer func() {
		if cerr := file.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			_ = os.Remove(absTempFileNameClean)
		}
	}()

	// Create a hash.Hash to compute the checksum
	h := sha256.New()

	// Create a multi-writer to write to both the file and the hash
	multiWriter := io.MultiWriter(file, h)

	// Copy from reader to both file and hash
	if _, err := io.Copy(multiWriter, reader); err != nil {
		return "", &IOError{
			"writing artifact content",
			err,
		}
	}

	// Generate version hash
	versionHash := hex.EncodeToString(h.Sum(nil))

	// Rename the temp file to the final path
	finalPath := r.getArtifactPath(pkg, versionHash)
	// Ensure the final directory exists before renaming
	finalDir := filepath.Dir(finalPath)

	//nolint:gosec,mnd // Directory permissions 0755 are intentional
	if err := os.MkdirAll(finalDir, 0o755); err != nil {
		return "", &IOError{
			"creating artifact directory",
			err,
		}
	}
	if err := os.Rename(absTempFileNameClean, finalPath); err != nil {
		return "", &IOError{
			"renaming artifact file",
			err,
		}
	}

	return versionHash, nil
}

// GetArtifact retrieves an artifact by identifier
func (r *FilesystemRegistry) GetArtifact(
	pkg *proto_gen.PackageName,
	hash string,
) ([]byte, error) {
	artifactPath := r.getArtifactPath(pkg, hash)
	//nolint:gosec // G304: File path is constructed internally and validated
	content, err := os.ReadFile(artifactPath)
	if err != nil {
		return nil, &IOError{
			"reading artifact",
			err,
		}
	}

	return content, nil
}

// DeleteArtifact deletes an artifact by identifier
func (r *FilesystemRegistry) DeleteArtifact(
	pkg *proto_gen.PackageName,
	hash string,
) error {
	// Remove the file
	artifactPath := r.getArtifactPath(pkg, hash)
	if err := os.Remove(artifactPath); err != nil {
		return &IOError{
			"deleting artifact",
			err,
		}
	}

	return nil
}

// getArtifactPath returns the file path for an artifact
func (r *FilesystemRegistry) getArtifactPath(
	pkg *proto_gen.PackageName,
	versionHash string,
) string {
	return filepath.Join(
		r.baseDir,
		pkg.Namespace,
		pkg.Name,
		versionHash+".wasm",
	)
}
