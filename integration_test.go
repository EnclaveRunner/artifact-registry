package main

import (
	"artifact-registry/config"
	"artifact-registry/orm"
	"artifact-registry/proto_gen"
	"artifact-registry/registry"
	"artifact-registry/registry/memoryRegistry"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/EnclaveRunner/shareddeps"
	configShareddeps "github.com/EnclaveRunner/shareddeps/config"
	"github.com/stretchr/testify/assert"
)

var (
	usedPorts     = map[int]bool{}
	usedPortsLock sync.Mutex
	dbInitOnce    sync.Once
	sharedDB      orm.DB
)

func configureServer(
	t *testing.T,
	storageDir string,
) (registryClient proto_gen.RegistryServiceClient, startServer func()) {
	t.Helper()
	port := getAvailablePort(t)

	defaults := []configShareddeps.DefaultValue{
		{Key: "port", Value: port},
		{Key: "production_environment", Value: false},
		{Key: "log_level", Value: "debug"},
		{Key: "human_readable_output", Value: true},
		{Key: "storage_dir", Value: storageDir},

		{Key: "database.port", Value: 5432},
		{Key: "database.host", Value: "localhost"},
		{Key: "database.sslmode", Value: "disable"},
		{Key: "database.username", Value: "enclave_user"},
		{Key: "database.password", Value: "enclave_password"},
		{Key: "database.database", Value: "enclave_db"},
	}

	cfg := &config.AppConfig{}
	err := configShareddeps.PopulateAppConfig(
		cfg,
		"artifact-registry",
		"testing",
		defaults...)
	assert.NoError(t, err)

	memRegistry := memoryRegistry.New()

	// Initialize DB only once across all tests to avoid migration conflicts
	dbInitOnce.Do(func() {
		sharedDB = orm.InitDB(cfg)
	})

	server := shareddeps.InitGRPCServer()

	proto_gen.RegisterRegistryServiceServer(
		server,
		registry.NewServer(memRegistry, sharedDB),
	)

	client := proto_gen.NewRegistryServiceClient(
		shareddeps.InitGRPCClient(
			"localhost",
			port,
		),
	)

	return client, func() {
		defer func() {
			usedPortsLock.Lock()
			usedPorts[port] = false
			usedPortsLock.Unlock()
		}()
		shareddeps.StartGRPCServer(cfg, server)
	}
}

func getAvailablePort(t *testing.T) int {
	t.Helper()
	usedPortsLock.Lock()
	defer usedPortsLock.Unlock()

	for port := 1024; port <= 65535; port++ {
		if usedPorts[port] {
			continue
		}
		//nolint:noctx // Allow contextless listen for port check in test
		ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err != nil {
			continue
		}
		_ = ln.Close()
		usedPorts[port] = true

		return port
	}

	t.Fatal("no available ports found")

	return 0
}

// TestUploadAndGetArtifact tests basic artifact upload and retrieval
func TestUploadAndGetArtifact(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	// Upload an artifact
	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "upload-get-test",
		Name:   "myartifact",
	}
	tags := []string{"v1.0.0", "latest"}
	content := []byte("test artifact content")

	artifact := uploadArtifact(t, client, fqn, tags, content)

	assert.NotNil(t, artifact)
	assert.Equal(t, fqn.Source, artifact.Fqn.Source)
	assert.Equal(t, fqn.Author, artifact.Fqn.Author)
	assert.Equal(t, fqn.Name, artifact.Fqn.Name)
	assert.NotEmpty(t, artifact.VersionHash)
	assert.ElementsMatch(t, tags, artifact.Tags)
	assert.NotNil(t, artifact.Metadata)
	assert.Equal(t, int64(0), artifact.Metadata.Pulls)

	// Get artifact by version hash
	getReq := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: artifact.VersionHash,
		},
	}

	retrieved, err := client.GetArtifact(t.Context(), getReq)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, artifact.VersionHash, retrieved.VersionHash)
	assert.ElementsMatch(t, tags, retrieved.Tags)
}

// TestUploadAndPullArtifact tests uploading and pulling artifact content
func TestUploadAndPullArtifact(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	fqn := &proto_gen.FullyQualifiedName{
		Source: "gitlab",
		Author: "upload-pull-test",
		Name:   "testapp",
	}
	tags := []string{"v2.0.0"}
	content := []byte("This is test artifact content with some data")

	artifact := uploadArtifact(t, client, fqn, tags, content)

	// Pull artifact by version hash
	pullReq := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: artifact.VersionHash,
		},
	}

	pulledContent := pullArtifact(t, client, pullReq)
	assert.Equal(t, content, pulledContent)

	// Verify pull count increased
	retrieved, err := client.GetArtifact(t.Context(), pullReq)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), retrieved.Metadata.Pulls)

	// Pull again and verify count increases
	_ = pullArtifact(t, client, pullReq)
	retrieved, err = client.GetArtifact(t.Context(), pullReq)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), retrieved.Metadata.Pulls)
}

// TestPullArtifactByTag tests pulling artifact using tag identifier
func TestPullArtifactByTag(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "pull-by-tag-test",
		Name:   "project",
	}
	tags := []string{"stable", "production"}
	content := []byte("production artifact content")

	artifact := uploadArtifact(t, client, fqn, tags, content)

	// Pull by tag
	pullReq := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_Tag{
			Tag: "stable",
		},
	}

	pulledContent := pullArtifact(t, client, pullReq)
	assert.Equal(t, content, pulledContent)

	// Pull by another tag
	pullReq.Identifier = &proto_gen.ArtifactIdentifier_Tag{
		Tag: "production",
	}
	pulledContent = pullArtifact(t, client, pullReq)
	assert.Equal(t, content, pulledContent)

	// Verify pull count is 2
	getReq := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: artifact.VersionHash,
		},
	}
	retrieved, err := client.GetArtifact(t.Context(), getReq)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), retrieved.Metadata.Pulls)
}

// TestQueryArtifacts tests artifact querying with different filters
func TestQueryArtifacts(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	// Upload multiple artifacts
	artifacts := []struct {
		fqn     *proto_gen.FullyQualifiedName
		tags    []string
		content []byte
	}{
		{
			fqn: &proto_gen.FullyQualifiedName{
				Source: "bitbucket",
				Author: "query-test-user1",
				Name:   "query-app1",
			},
			tags:    []string{"v1.0.0"},
			content: []byte("app1 content"),
		},
		{
			fqn: &proto_gen.FullyQualifiedName{
				Source: "bitbucket",
				Author: "query-test-user1",
				Name:   "query-app2",
			},
			tags:    []string{"v1.0.0"},
			content: []byte("app2 content"),
		},
		{
			fqn: &proto_gen.FullyQualifiedName{
				Source: "bitbucket",
				Author: "query-test-user2",
				Name:   "query-app1",
			},
			tags:    []string{"v2.0.0"},
			content: []byte("user2 app1 content"),
		},
		{
			fqn: &proto_gen.FullyQualifiedName{
				Source: "gitlab",
				Author: "query-test-user1",
				Name:   "query-app1",
			},
			tags:    []string{"v1.5.0"},
			content: []byte("gitlab app1 content"),
		},
	}

	for _, a := range artifacts {
		uploadArtifact(t, client, a.fqn, a.tags, a.content)
	}

	// Query by source
	source := "bitbucket"
	resp, err := client.QueryArtifacts(
		t.Context(),
		&proto_gen.ArtifactQuery{Source: &source},
	)
	assert.NoError(t, err)
	assert.Len(t, resp.Artifacts, 3)

	// Query by author
	author := "query-test-user1"
	resp, err = client.QueryArtifacts(
		t.Context(),
		&proto_gen.ArtifactQuery{Author: &author},
	)
	assert.NoError(t, err)
	assert.Len(t, resp.Artifacts, 3)

	// Query by name
	name := "query-app1"
	resp, err = client.QueryArtifacts(
		t.Context(),
		&proto_gen.ArtifactQuery{Name: &name},
	)
	assert.NoError(t, err)
	assert.Len(t, resp.Artifacts, 3)

	// Query by source and author
	resp, err = client.QueryArtifacts(t.Context(), &proto_gen.ArtifactQuery{
		Source: &source,
		Author: &author,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Artifacts, 2)

	// Query by full FQN
	resp, err = client.QueryArtifacts(t.Context(), &proto_gen.ArtifactQuery{
		Source: &source,
		Author: &author,
		Name:   &name,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Artifacts, 1)
	assert.Equal(t, "bitbucket", resp.Artifacts[0].Fqn.Source)
	assert.Equal(t, "query-test-user1", resp.Artifacts[0].Fqn.Author)
	assert.Equal(t, "query-app1", resp.Artifacts[0].Fqn.Name)
}

// TestAddAndRemoveTags tests tag management operations
func TestAddAndRemoveTags(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "tag-test-user",
		Name:   "tag-test-app",
	}
	initialTags := []string{"v1.0.0"}
	content := []byte("test content")

	artifact := uploadArtifact(t, client, fqn, initialTags, content)

	// Add a new tag
	addReq := &proto_gen.AddRemoveTagRequest{
		Fqn:         fqn,
		VersionHash: artifact.VersionHash,
		Tag:         "latest",
	}

	updated, err := client.AddTag(t.Context(), addReq)
	assert.NoError(t, err)
	assert.Contains(t, updated.Tags, "latest")
	assert.Contains(t, updated.Tags, "v1.0.0")
	assert.Len(t, updated.Tags, 2)

	// Add another tag
	addReq.Tag = "stable"
	updated, err = client.AddTag(t.Context(), addReq)
	assert.NoError(t, err)
	assert.Contains(t, updated.Tags, "stable")
	assert.Len(t, updated.Tags, 3)

	// Remove a tag
	removeReq := &proto_gen.AddRemoveTagRequest{
		Fqn:         fqn,
		VersionHash: artifact.VersionHash,
		Tag:         "v1.0.0",
	}

	updated, err = client.RemoveTag(t.Context(), removeReq)
	assert.NoError(t, err)
	assert.NotContains(t, updated.Tags, "v1.0.0")
	assert.Contains(t, updated.Tags, "latest")
	assert.Contains(t, updated.Tags, "stable")
	assert.Len(t, updated.Tags, 2)
}

func TestDeleteTagFromNonExistentArtifact(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	// Create an artifact
	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "nonexistent-tag-test",
		Name:   "nonexistent-tag-app",
	}
	tags := []string{"v1.0.0"}
	content := []byte("test content")

	artifact := uploadArtifact(t, client, fqn, tags, content)

	// Delete tag
	removeReq := &proto_gen.AddRemoveTagRequest{
		Fqn:         fqn,
		VersionHash: "non existent hash",
		Tag:         "v1.0.0",
	}

	_, err := client.RemoveTag(t.Context(), removeReq)
	assert.Error(t, err)

	// Verify original artifact is unaffected
	getReq := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: artifact.VersionHash,
		},
	}
	retrieved, err := client.GetArtifact(t.Context(), getReq)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Contains(t, retrieved.Tags, "v1.0.0")

	// Remove artifact
	_, err = client.DeleteArtifact(t.Context(), getReq)
	assert.NoError(t, err)
}

func TestDeleteTagThatDoesNotExist(t *testing.T) {
	t.Parallel()
	// Create an artifact
	client, startServer := configureServer(t, t.TempDir())
	go startServer()
	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "nonexistent-tag-test",
		Name:   "nonexistent-tag-app2",
	}

	content := []byte("test content")
	artifact := uploadArtifact(t, client, fqn, nil, content)

	// Delete tag that does not exist
	removeReq := &proto_gen.AddRemoveTagRequest{
		Fqn:         fqn,
		VersionHash: artifact.VersionHash,
		Tag:         "nonexistent-tag",
	}

	_, err := client.RemoveTag(t.Context(), removeReq)
	assert.Error(t, err)

	// Delete artifact
	getReq := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: artifact.VersionHash,
		},
	}

	_, err = client.DeleteArtifact(t.Context(), getReq)
	assert.NoError(t, err)
}

// TestDeleteArtifact tests artifact deletion
func TestDeleteArtifact(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "delete-test-user",
		Name:   "delete-test-app",
	}
	tags := []string{"v1.0.0"}
	content := []byte("to be deleted")

	artifact := uploadArtifact(t, client, fqn, tags, content)

	// Verify artifact exists
	getReq := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: artifact.VersionHash,
		},
	}

	retrieved, err := client.GetArtifact(t.Context(), getReq)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)

	// Delete the artifact
	deleted, err := client.DeleteArtifact(t.Context(), getReq)
	assert.NoError(t, err)
	assert.NotNil(t, deleted)
	assert.Equal(t, artifact.VersionHash, deleted.VersionHash)

	// Verify artifact no longer exists
	_, err = client.GetArtifact(t.Context(), getReq)
	assert.Error(t, err)

	// Verify pull fails
	pullStream, err := client.PullArtifact(t.Context(), getReq)
	assert.NoError(t, err)
	_, err = pullStream.Recv()
	assert.Error(t, err)
}

// TestDeleteArtifactByTag tests deletion using tag identifier
func TestDeleteArtifactByTag(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "delete-by-tag-test",
		Name:   "delete-by-tag-app",
	}
	tags := []string{"v1.0.0", "deleteme"}
	content := []byte("to be deleted by tag")

	artifact := uploadArtifact(t, client, fqn, tags, content)

	// Delete by tag
	deleteReq := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_Tag{
			Tag: "deleteme",
		},
	}

	deleted, err := client.DeleteArtifact(t.Context(), deleteReq)
	assert.NoError(t, err)
	assert.Equal(t, artifact.VersionHash, deleted.VersionHash)

	// Verify artifact is deleted
	getReq := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: artifact.VersionHash,
		},
	}
	_, err = client.GetArtifact(t.Context(), getReq)
	assert.Error(t, err)
}

// TestMultipleVersions tests uploading multiple versions of the same artifact
func TestMultipleVersions(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "multiversion-test",
		Name:   "multiversion-app",
	}

	// Upload version 1
	v1Tags := []string{"v1.0.0"}
	v1Content := []byte("version 1 content")
	v1 := uploadArtifact(t, client, fqn, v1Tags, v1Content)

	// Upload version 2
	v2Tags := []string{"v2.0.0", "latest"}
	v2Content := []byte("version 2 content - updated")
	v2 := uploadArtifact(t, client, fqn, v2Tags, v2Content)

	// Verify different hashes
	assert.NotEqual(t, v1.VersionHash, v2.VersionHash)

	// Query all versions
	name := fqn.Name
	resp, err := client.QueryArtifacts(
		t.Context(),
		&proto_gen.ArtifactQuery{Name: &name},
	)
	assert.NoError(t, err)
	assert.Len(t, resp.Artifacts, 2)

	// Pull v1
	v1Req := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_Tag{
			Tag: "v1.0.0",
		},
	}
	pulled := pullArtifact(t, client, v1Req)
	assert.Equal(t, v1Content, pulled)

	// Pull v2
	v2Req := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_Tag{
			Tag: "v2.0.0",
		},
	}
	pulled = pullArtifact(t, client, v2Req)
	assert.Equal(t, v2Content, pulled)

	// Pull latest (should be v2)
	latestReq := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_Tag{
			Tag: "latest",
		},
	}
	pulled = pullArtifact(t, client, latestReq)
	assert.Equal(t, v2Content, pulled)
}

// TestLargeArtifact tests uploading and downloading a large artifact
func TestLargeArtifact(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "large-file-test",
		Name:   "large-file-app",
	}
	tags := []string{"v1.0.0"}

	// Create 10MB of content
	largeContent := make([]byte, 10*1024*1024)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	artifact := uploadArtifact(t, client, fqn, tags, largeContent)
	assert.NotEmpty(t, artifact.VersionHash)

	// Pull and verify
	pullReq := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: artifact.VersionHash,
		},
	}

	pulled := pullArtifact(t, client, pullReq)
	assert.Equal(t, largeContent, pulled)
}

// TestInvalidFQN tests validation of FullyQualifiedName
func TestInvalidFQN(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	testCases := []struct {
		name string
		fqn  *proto_gen.FullyQualifiedName
	}{
		{
			name: "missing source",
			fqn: &proto_gen.FullyQualifiedName{
				Source: "",
				Author: "user",
				Name:   "app",
			},
		},
		{
			name: "missing author",
			fqn: &proto_gen.FullyQualifiedName{
				Source: "github",
				Author: "",
				Name:   "app",
			},
		},
		{
			name: "missing name",
			fqn: &proto_gen.FullyQualifiedName{
				Source: "github",
				Author: "user",
				Name:   "",
			},
		},
		{
			name: "nil fqn",
			fqn:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Try GetArtifact with invalid FQN
			req := &proto_gen.ArtifactIdentifier{
				Fqn: tc.fqn,
				Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
					VersionHash: "somehash",
				},
			}

			_, err := client.GetArtifact(t.Context(), req)
			assert.Error(t, err)
		})
	}
}

// TestInvalidIdentifiers tests validation of artifact identifiers
func TestInvalidIdentifiers(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "invalid-identifier-test",
		Name:   "invalid-identifier-app",
	}

	// Empty version hash
	req := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: "",
		},
	}
	_, err := client.GetArtifact(t.Context(), req)
	assert.Error(t, err)

	// Empty tag
	req.Identifier = &proto_gen.ArtifactIdentifier_Tag{
		Tag: "",
	}
	_, err = client.GetArtifact(t.Context(), req)
	assert.Error(t, err)
}

// TestNonExistentArtifact tests operations on non-existent artifacts
func TestNonExistentArtifact(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "nonexistent-test",
		Name:   "nonexistent-app",
	}

	// Try to get non-existent artifact
	req := &proto_gen.ArtifactIdentifier{
		Fqn: fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: "nonexistenthash",
		},
	}

	_, err := client.GetArtifact(t.Context(), req)
	assert.Error(t, err)

	// Try to pull non-existent artifact
	stream, err := client.PullArtifact(t.Context(), req)
	assert.NoError(t, err)
	_, err = stream.Recv()
	assert.Error(t, err)

	// Try to delete non-existent artifact
	_, err = client.DeleteArtifact(t.Context(), req)
	assert.Error(t, err)
}

// TestInvalidTagOperations tests error cases for tag management
func TestInvalidTagOperations(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "invalid-tag-test",
		Name:   "invalid-tag-app",
	}

	// Try to add tag to non-existent artifact
	addReq := &proto_gen.AddRemoveTagRequest{
		Fqn:         fqn,
		VersionHash: "nonexistenthash",
		Tag:         "latest",
	}

	_, err := client.AddTag(t.Context(), addReq)
	assert.Error(t, err)

	// Try to remove tag from non-existent artifact
	_, err = client.RemoveTag(t.Context(), addReq)
	assert.Error(t, err)

	// Try to add empty tag
	artifact := uploadArtifact(t, client, fqn, []string{"v1.0.0"}, []byte("test"))
	addReq.VersionHash = artifact.VersionHash
	addReq.Tag = ""

	_, err = client.AddTag(t.Context(), addReq)
	assert.Error(t, err)
}

// TestArtifactWithMultipleTags tests artifact with multiple tags from the start
func TestArtifactWithMultipleTags(t *testing.T) {
	t.Parallel()

	client, startServer := configureServer(t, t.TempDir())
	go startServer()

	fqn := &proto_gen.FullyQualifiedName{
		Source: "github",
		Author: "multiple-tags-test",
		Name:   "multiple-tags-app",
	}
	tags := []string{"v1.0.0", "latest", "stable", "production"}
	content := []byte("multi-tag content")

	artifact := uploadArtifact(t, client, fqn, tags, content)
	assert.ElementsMatch(t, tags, artifact.Tags)

	// Pull by each tag and verify
	for _, tag := range tags {
		pullReq := &proto_gen.ArtifactIdentifier{
			Fqn: fqn,
			Identifier: &proto_gen.ArtifactIdentifier_Tag{
				Tag: tag,
			},
		}

		pulled := pullArtifact(t, client, pullReq)
		assert.Equal(t, content, pulled)
	}
}

// Helper function to upload an artifact
func uploadArtifact(
	t *testing.T,
	client proto_gen.RegistryServiceClient,
	fqn *proto_gen.FullyQualifiedName,
	tags []string,
	content []byte,
) *proto_gen.Artifact {
	t.Helper()

	stream, err := client.UploadArtifact(t.Context())
	assert.NoError(t, err)

	// Send metadata
	err = stream.Send(&proto_gen.UploadArtifactRequest{
		Request: &proto_gen.UploadArtifactRequest_Metadata{
			Metadata: &proto_gen.UploadMetadata{
				Fqn:  fqn,
				Tags: tags,
			},
		},
	})
	assert.NoError(t, err)

	// Send content in chunks
	chunkSize := 1024 * 1024 // 1MB chunks
	for i := 0; i < len(content); i += chunkSize {
		end := i + chunkSize
		if end > len(content) {
			end = len(content)
		}

		err = stream.Send(&proto_gen.UploadArtifactRequest{
			Request: &proto_gen.UploadArtifactRequest_Content{
				Content: &proto_gen.ArtifactContent{
					Data: content[i:end],
				},
			},
		})
		assert.NoError(t, err)
	}

	artifact, err := stream.CloseAndRecv()
	assert.NoError(t, err)
	assert.NotNil(t, artifact)

	return artifact
}

// Helper function to pull an artifact
func pullArtifact(
	t *testing.T,
	client proto_gen.RegistryServiceClient,
	req *proto_gen.ArtifactIdentifier,
) []byte {
	t.Helper()

	stream, err := client.PullArtifact(t.Context(), req)
	assert.NoError(t, err)

	var content []byte
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break
		}
		content = append(content, chunk.Data...)
	}

	return content
}
