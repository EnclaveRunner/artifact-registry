package filesystemRegistry

import (
	"artifact-registry/proto_gen"
	"os"
	"testing"
)

func TestSimpleFilesystemRegistry(t *testing.T) {
	// Create temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "registry-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create registry
	registry, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create registry: %v", err)
	}

	// Test artifact
	artifact := &proto_gen.Artifact{
		Fqn: &proto_gen.FullQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
		},
		Content: []byte("test content"),
	}

	// Test StoreArtifact
	t.Run("StoreArtifact", func(t *testing.T) {
		_, err := registry.StoreArtifact(artifact.Fqn, artifact.Content)
		if err != nil {
			t.Fatalf("Failed to store artifact: %v", err)
		}

		// Verify version hash was generated
		if artifact.VersionHash == "" {
			t.Error("Version hash was not generated")
		}
	})

	// Test GetArtifact by version hash
	t.Run("GetArtifactByHash", func(t *testing.T) {
		id := &proto_gen.ArtifactIdentifier{
			Fqn: artifact.Fqn,
			Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
				VersionHash: artifact.VersionHash,
			},
		}

		retrieved, err := registry.GetArtifact(id)
		if err != nil {
			t.Fatalf("Failed to get artifact: %v", err)
		}

		if string(retrieved.Content) != "test content" {
			t.Error("Content mismatch")
		}
	})

	// Test DeleteArtifact
	t.Run("DeleteArtifact", func(t *testing.T) {
		id := &proto_gen.ArtifactIdentifier{
			Fqn: artifact.Fqn,
			Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
				VersionHash: artifact.VersionHash,
			},
		}

		deleted, err := registry.DeleteArtifact(id)
		if err != nil {
			t.Fatalf("Failed to delete artifact: %v", err)
		}

		if string(deleted.Content) != "test content" {
			t.Error("Wrong artifact returned from delete")
		}

		// Verify artifact is gone
		_, err = registry.GetArtifact(id)
		if err == nil {
			t.Error("Expected error when getting deleted artifact")
		}
	})

	// Test QueryArtifacts (returns empty in simple implementation)
	t.Run("QueryArtifacts", func(t *testing.T) {
		query := &proto_gen.ArtifactQuery{}
		results, err := registry.QueryArtifacts(query)
		if err != nil {
			t.Fatalf("Failed to query artifacts: %v", err)
		}

		if len(results) != 0 {
			t.Errorf("Expected 0 results in simple implementation, got %d", len(results))
		}
	})
}
