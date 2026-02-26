package filesystemRegistry

import (
	"artifact-registry/config"
	"artifact-registry/proto_gen"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	sharedepsConfig "github.com/EnclaveRunner/shareddeps/config"
)

func TestFilesystemRegistry(t *testing.T) {
	t.Parallel()

	// Test StoreArtifact - should compute hash and store file
	t.Run("StoreArtifact", func(t *testing.T) {
		t.Parallel()

		// Setup
		tmpDir, registry := setupTest(t)
		//nolint:errcheck // defer in test
		defer os.RemoveAll(tmpDir)

		fqn := &proto_gen.PackageName{
			Namespace: "testuser",
			Name:      "testapp",
		}
		content := []byte("test content for artifact")

		versionHash, err := registry.StoreArtifact(fqn, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("Failed to store artifact: %v", err)
		}

		// Verify version hash was generated and is a valid hex string
		if versionHash == "" {
			t.Error("Version hash was not generated")
		}
		if len(versionHash) != 64 { // SHA256 hex string should be 64 characters
			t.Errorf("Expected version hash length 64, got %d", len(versionHash))
		}

		// Verify that the artifact file was actually created on disk
		expectedPath := filepath.Join(
			tmpDir,
			fqn.Namespace,
			fqn.Name,
			versionHash+".wasm",
		)
		if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
			t.Errorf(
				"Artifact file was not created at expected path: %s",
				expectedPath,
			)
		}
	})

	// Test GetArtifact by version hash - should retrieve the exact same content
	t.Run("GetArtifactByHash", func(t *testing.T) {
		t.Parallel()

		// Setup
		tmpDir, registry := setupTest(t)
		//nolint:errcheck // defer in test
		defer os.RemoveAll(tmpDir)

		fqn := &proto_gen.PackageName{
			Namespace: "testuser",
			Name:      "testapp",
		}
		content := []byte("test content for artifact")

		// Store artifact first
		storedVersionHash, err := registry.StoreArtifact(
			fqn,
			bytes.NewReader(content),
		)
		if err != nil {
			t.Fatalf("Failed to store artifact: %v", err)
		}

		retrieved, err := registry.GetArtifact(fqn, storedVersionHash)
		if err != nil {
			t.Fatalf("Failed to get artifact: %v", err)
		}

		if !bytes.Equal(retrieved, content) {
			t.Errorf(
				"Content mismatch. Expected: %q, Got: %q",
				string(content),
				string(retrieved),
			)
		}

		if len(retrieved) != len(content) {
			t.Errorf(
				"Content length mismatch. Expected: %d, Got: %d",
				len(content),
				len(retrieved),
			)
		}
	})

	// Test GetArtifact with non-existent hash - should return error
	t.Run("GetNonExistentArtifact", func(t *testing.T) {
		t.Parallel()

		// Setup
		tmpDir, registry := setupTest(t)
		//nolint:errcheck // defer in test
		defer os.RemoveAll(tmpDir)

		fqn := &proto_gen.PackageName{
			Namespace: "testuser",
			Name:      "testapp",
		}

		nonExistentHash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		_, err := registry.GetArtifact(fqn, nonExistentHash)
		if err == nil {
			t.Error("Expected error when getting non-existent artifact, but got none")
		}
	})

	// Test StoreArtifact with different content - should generate different hash
	t.Run("StoreArtifactDifferentContent", func(t *testing.T) {
		t.Parallel()

		// Setup
		tmpDir, registry := setupTest(t)
		//nolint:errcheck // defer in test
		defer os.RemoveAll(tmpDir)

		fqn := &proto_gen.PackageName{
			Namespace: "testuser",
			Name:      "testapp",
		}
		content := []byte("test content for artifact")
		differentContent := []byte("different test content")

		// Store first artifact
		storedVersionHash, err := registry.StoreArtifact(
			fqn,
			bytes.NewReader(content),
		)
		if err != nil {
			t.Fatalf("Failed to store first artifact: %v", err)
		}

		versionHash2, err := registry.StoreArtifact(
			fqn,
			bytes.NewReader(differentContent),
		)
		if err != nil {
			t.Fatalf("Failed to store second artifact: %v", err)
		}

		if versionHash2 == storedVersionHash {
			t.Error("Different content should generate different version hash")
		}

		// Verify we can retrieve both artifacts
		content1, err := registry.GetArtifact(fqn, storedVersionHash)
		if err != nil {
			t.Fatalf("Failed to get first artifact: %v", err)
		}

		content2, err := registry.GetArtifact(fqn, versionHash2)
		if err != nil {
			t.Fatalf("Failed to get second artifact: %v", err)
		}

		if bytes.Equal(content1, content2) {
			t.Error("Retrieved contents should be different")
		}
	})

	// Test DeleteArtifact - should remove file and make it unavailable
	t.Run("DeleteArtifact", func(t *testing.T) {
		t.Parallel()

		// Setup
		tmpDir, registry := setupTest(t)
		//nolint:errcheck // defer in test
		defer os.RemoveAll(tmpDir)

		fqn := &proto_gen.PackageName{
			Namespace: "testuser",
			Name:      "testapp",
		}
		content := []byte("test content for artifact")

		// Store artifact first
		storedVersionHash, err := registry.StoreArtifact(
			fqn,
			bytes.NewReader(content),
		)
		if err != nil {
			t.Fatalf("Failed to store artifact: %v", err)
		}

		// Verify artifact exists before deletion
		_, err = registry.GetArtifact(fqn, storedVersionHash)
		if err != nil {
			t.Fatalf("Artifact should exist before deletion: %v", err)
		}

		// Delete the artifact
		err = registry.DeleteArtifact(fqn, storedVersionHash)
		if err != nil {
			t.Fatalf("Failed to delete artifact: %v", err)
		}

		// Verify artifact is gone from filesystem
		expectedPath := filepath.Join(
			tmpDir,
			fqn.Namespace,
			fqn.Name,
			storedVersionHash+".wasm",
		)
		if _, err := os.Stat(expectedPath); !os.IsNotExist(err) {
			t.Error("Artifact file should have been deleted from filesystem")
		}

		// Verify artifact cannot be retrieved
		_, err = registry.GetArtifact(fqn, storedVersionHash)
		if err == nil {
			t.Error("Expected error when getting deleted artifact, but got none")
		}
	})

	// Test DeleteArtifact with non-existent hash - should return error
	t.Run("DeleteNonExistentArtifact", func(t *testing.T) {
		t.Parallel()

		// Setup
		tmpDir, registry := setupTest(t)
		//nolint:errcheck // defer in test
		defer os.RemoveAll(tmpDir)

		fqn := &proto_gen.PackageName{
			Namespace: "testuser",
			Name:      "testapp",
		}

		nonExistentHash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		err := registry.DeleteArtifact(fqn, nonExistentHash)
		if err == nil {
			t.Error(
				"Expected error when deleting non-existent artifact, but got none",
			)
		}
	})

	// Test directory structure creation
	t.Run("DirectoryStructure", func(t *testing.T) {
		t.Parallel()

		// Setup
		tmpDir, registry := setupTest(t)
		//nolint:errcheck // defer in test
		defer os.RemoveAll(tmpDir)

		// Store artifact with complex package name
		complexFqn := &proto_gen.PackageName{
			Namespace: "complex-author",
			Name:      "complex-name-with-dashes",
		}

		complexContent := []byte("content for complex artifact")
		versionHash, err := registry.StoreArtifact(
			complexFqn,
			bytes.NewReader(complexContent),
		)
		if err != nil {
			t.Fatalf("Failed to store complex artifact: %v", err)
		}

		// Verify directory structure was created correctly
		expectedDir := filepath.Join(
			tmpDir,
			complexFqn.Namespace,
			complexFqn.Name,
		)
		if info, err := os.Stat(expectedDir); err != nil {
			t.Errorf(
				"Expected directory not created: %s, error: %v",
				expectedDir,
				err,
			)
		} else if !info.IsDir() {
			t.Errorf("Expected path is not a directory: %s", expectedDir)
		}

		// Clean up
		_ = registry.DeleteArtifact(complexFqn, versionHash)
	})
}

// setupTest creates a temporary directory and registry for testing
func setupTest(t *testing.T) (string, *FilesystemRegistry) {
	t.Helper()

	cfg := &config.AppConfig{}
	_ = sharedepsConfig.PopulateAppConfig(
		cfg,
		"artifact-registry",
		"TESTING",
		config.Defaults...)

	tmpDir, err := os.MkdirTemp("", "registry-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	registry, err := New(tmpDir)
	if err != nil {
		//nolint:errcheck,gosec // defer in test
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create registry: %v", err)
	}

	return tmpDir, registry
}
