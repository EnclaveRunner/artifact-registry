package memoryRegistry

import (
	"artifact-registry/proto_gen"
	"bytes"
	"sync"
	"testing"
)

func TestMemoryRegistry(t *testing.T) {
	t.Parallel()

	// Test StoreArtifact - should compute hash and store content
	t.Run("StoreArtifact", func(t *testing.T) {
		t.Parallel()

		registry := New()
		fqn := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
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

		// Verify the artifact count increased
		if count := registry.Count(); count != 1 {
			t.Errorf("Expected 1 artifact in registry, got %d", count)
		}
	})

	// Test GetArtifact by version hash - should retrieve the exact same content
	t.Run("GetArtifactByHash", func(t *testing.T) {
		t.Parallel()

		registry := New()
		fqn := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
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

		registry := New()
		fqn := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
		}

		nonExistentHash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		_, err := registry.GetArtifact(fqn, nonExistentHash)
		if err == nil {
			t.Error("Expected error when getting non-existent artifact, but got none")
		}
		if err != ErrArtifactNotFound {
			t.Errorf("Expected ErrArtifactNotFound, got: %v", err)
		}
	})

	// Test StoreArtifact with different content - should generate different hash
	t.Run("StoreArtifactDifferentContent", func(t *testing.T) {
		t.Parallel()

		registry := New()
		fqn := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
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

		// Verify both artifacts are stored
		if count := registry.Count(); count != 2 {
			t.Errorf("Expected 2 artifacts in registry, got %d", count)
		}
	})

	// Test DeleteArtifact - should remove artifact and make it unavailable
	t.Run("DeleteArtifact", func(t *testing.T) {
		t.Parallel()

		registry := New()
		fqn := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
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

		// Verify artifact count decreased
		if count := registry.Count(); count != 0 {
			t.Errorf("Expected 0 artifacts in registry after deletion, got %d", count)
		}

		// Verify artifact cannot be retrieved
		_, err = registry.GetArtifact(fqn, storedVersionHash)
		if err == nil {
			t.Error("Expected error when getting deleted artifact, but got none")
		}
		if err != ErrArtifactNotFound {
			t.Errorf("Expected ErrArtifactNotFound, got: %v", err)
		}
	})

	// Test DeleteArtifact with non-existent hash - should return error
	t.Run("DeleteNonExistentArtifact", func(t *testing.T) {
		t.Parallel()

		registry := New()
		fqn := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
		}

		nonExistentHash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		err := registry.DeleteArtifact(fqn, nonExistentHash)
		if err == nil {
			t.Error(
				"Expected error when deleting non-existent artifact, but got none",
			)
		}
	})

	// Test storage key generation with complex FQN
	t.Run("ComplexFQN", func(t *testing.T) {
		t.Parallel()

		registry := New()

		// Store artifact with complex FQN
		complexFqn := &proto_gen.FullyQualifiedName{
			Source: "complex.domain.com",
			Author: "complex-author",
			Name:   "complex-name-with-dashes",
		}

		complexContent := []byte("content for complex artifact")
		versionHash, err := registry.StoreArtifact(
			complexFqn,
			bytes.NewReader(complexContent),
		)
		if err != nil {
			t.Fatalf("Failed to store complex artifact: %v", err)
		}

		// Verify we can retrieve it
		retrieved, err := registry.GetArtifact(complexFqn, versionHash)
		if err != nil {
			t.Fatalf("Failed to get complex artifact: %v", err)
		}

		if !bytes.Equal(retrieved, complexContent) {
			t.Error("Retrieved content does not match original")
		}
	})

	// Test Clear functionality
	t.Run("Clear", func(t *testing.T) {
		t.Parallel()

		registry := New()
		fqn := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
		}

		// Store multiple artifacts
		for i := 0; i < 5; i++ {
			content := []byte("test content " + string(rune(i)))
			_, err := registry.StoreArtifact(fqn, bytes.NewReader(content))
			if err != nil {
				t.Fatalf("Failed to store artifact %d: %v", i, err)
			}
		}

		// Verify artifacts were stored
		if count := registry.Count(); count != 5 {
			t.Errorf("Expected 5 artifacts before clear, got %d", count)
		}

		// Clear the registry
		registry.Clear()

		// Verify all artifacts are gone
		if count := registry.Count(); count != 0 {
			t.Errorf("Expected 0 artifacts after clear, got %d", count)
		}
	})

	// Test that GetArtifact returns a copy (not a reference)
	t.Run("GetArtifactReturnsCopy", func(t *testing.T) {
		t.Parallel()

		registry := New()
		fqn := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
		}
		content := []byte("test content for artifact")

		// Store artifact
		versionHash, err := registry.StoreArtifact(fqn, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("Failed to store artifact: %v", err)
		}

		// Get artifact and modify it
		retrieved1, err := registry.GetArtifact(fqn, versionHash)
		if err != nil {
			t.Fatalf("Failed to get artifact: %v", err)
		}

		// Modify the retrieved content
		retrieved1[0] = 'X'

		// Get artifact again
		retrieved2, err := registry.GetArtifact(fqn, versionHash)
		if err != nil {
			t.Fatalf("Failed to get artifact second time: %v", err)
		}

		// Verify the second retrieval is not affected by modifications to the first
		if !bytes.Equal(retrieved2, content) {
			t.Error("Modifications to retrieved content affected stored content")
		}
	})

	// Test concurrent access
	t.Run("ConcurrentAccess", func(t *testing.T) {
		t.Parallel()

		registry := New()
		fqn := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
		}

		// Number of concurrent operations
		numOps := 100

		var wg sync.WaitGroup

		// Concurrent stores
		hashes := make([]string, numOps)
		wg.Add(numOps)
		for i := 0; i < numOps; i++ {
			go func(idx int) {
				defer wg.Done()
				content := []byte("concurrent content " + string(rune(idx)))
				hash, err := registry.StoreArtifact(fqn, bytes.NewReader(content))
				if err != nil {
					t.Errorf("Failed to store artifact %d: %v", idx, err)
				}
				hashes[idx] = hash
			}(i)
		}

		// Wait for stores to complete
		wg.Wait()

		// Concurrent reads
		wg.Add(numOps)
		for i := 0; i < numOps; i++ {
			go func(idx int) {
				defer wg.Done()
				if hashes[idx] != "" {
					_, err := registry.GetArtifact(fqn, hashes[idx])
					if err != nil {
						t.Errorf("Failed to get artifact %d: %v", idx, err)
					}
				}
			}(i)
		}

		wg.Wait()

		// Concurrent deletes
		wg.Add(numOps)
		for i := 0; i < numOps; i++ {
			go func(idx int) {
				defer wg.Done()
				if hashes[idx] != "" {
					err := registry.DeleteArtifact(fqn, hashes[idx])
					if err != nil {
						t.Errorf("Failed to delete artifact %d: %v", idx, err)
					}
				}
			}(i)
		}

		wg.Wait()

		// Verify all artifacts are deleted
		if count := registry.Count(); count != 0 {
			t.Errorf("Expected 0 artifacts after concurrent deletes, got %d", count)
		}
	})

	// Test empty content
	t.Run("EmptyContent", func(t *testing.T) {
		t.Parallel()

		registry := New()
		fqn := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
		}
		content := []byte{}

		// Store empty artifact
		versionHash, err := registry.StoreArtifact(fqn, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("Failed to store empty artifact: %v", err)
		}

		// Verify hash was generated
		if versionHash == "" {
			t.Error("Version hash was not generated for empty content")
		}

		// Retrieve empty artifact
		retrieved, err := registry.GetArtifact(fqn, versionHash)
		if err != nil {
			t.Fatalf("Failed to get empty artifact: %v", err)
		}

		if len(retrieved) != 0 {
			t.Errorf("Expected empty content, got %d bytes", len(retrieved))
		}
	})

	// Test same content produces same hash
	t.Run("SameContentSameHash", func(t *testing.T) {
		t.Parallel()

		registry := New()
		fqn := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "testuser",
			Name:   "testapp",
		}
		content := []byte("identical content")

		// Store same content twice
		hash1, err := registry.StoreArtifact(fqn, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("Failed to store first artifact: %v", err)
		}

		hash2, err := registry.StoreArtifact(fqn, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("Failed to store second artifact: %v", err)
		}

		if hash1 != hash2 {
			t.Errorf(
				"Same content should produce same hash. Got %s and %s",
				hash1,
				hash2,
			)
		}

		// Both should be retrievable
		retrieved1, err := registry.GetArtifact(fqn, hash1)
		if err != nil {
			t.Fatalf("Failed to get first artifact: %v", err)
		}

		retrieved2, err := registry.GetArtifact(fqn, hash2)
		if err != nil {
			t.Fatalf("Failed to get second artifact: %v", err)
		}

		if !bytes.Equal(retrieved1, retrieved2) {
			t.Error("Retrieved contents should be identical")
		}
	})

	// Test multiple artifacts with different FQNs
	t.Run("MultipleFQNs", func(t *testing.T) {
		t.Parallel()

		registry := New()

		fqn1 := &proto_gen.FullyQualifiedName{
			Source: "github.com",
			Author: "author1",
			Name:   "app1",
		}

		fqn2 := &proto_gen.FullyQualifiedName{
			Source: "gitlab.com",
			Author: "author2",
			Name:   "app2",
		}

		content := []byte("same content for both")

		// Store with different FQNs
		hash1, err := registry.StoreArtifact(fqn1, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("Failed to store artifact 1: %v", err)
		}

		hash2, err := registry.StoreArtifact(fqn2, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("Failed to store artifact 2: %v", err)
		}

		// Hashes should be the same (same content)
		if hash1 != hash2 {
			t.Errorf("Same content should produce same hash regardless of FQN")
		}

		// But they should be stored separately
		if count := registry.Count(); count != 2 {
			t.Errorf("Expected 2 artifacts (different FQNs), got %d", count)
		}

		// Both should be retrievable
		_, err = registry.GetArtifact(fqn1, hash1)
		if err != nil {
			t.Errorf("Failed to get artifact with fqn1: %v", err)
		}

		_, err = registry.GetArtifact(fqn2, hash2)
		if err != nil {
			t.Errorf("Failed to get artifact with fqn2: %v", err)
		}

		// Deleting one shouldn't affect the other
		err = registry.DeleteArtifact(fqn1, hash1)
		if err != nil {
			t.Fatalf("Failed to delete artifact 1: %v", err)
		}

		// fqn2 should still be retrievable
		_, err = registry.GetArtifact(fqn2, hash2)
		if err != nil {
			t.Errorf(
				"Artifact 2 should still exist after deleting artifact 1: %v",
				err,
			)
		}
	})
}
