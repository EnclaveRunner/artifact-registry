package procedures

import (
	proto "artifact-registry/proto_gen"
)

// registry interface defines the methods that any registry implementation must provide
type registry interface {
	StoreArtifact(fqn *proto.FullQualifiedName, content []byte) (string, error)
	GetArtifact(id *proto.ArtifactIdentifier) (*proto.Artifact, error)
	DeleteArtifact(id *proto.ArtifactIdentifier) (*proto.Artifact, error)
	QueryArtifacts(query *proto.ArtifactQuery) ([]*proto.Artifact, error)
}

type Server struct {
	proto.UnimplementedRegistryServiceServer
	registry registry
}

// NewServer creates a new server with the specified registry implementation
func NewServer(reg registry) *Server {
	return &Server{
		registry: reg,
	}
}
