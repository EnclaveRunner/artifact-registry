package registry

import (
	"artifact-registry/proto_gen"
)

// Registry interface defines the methods that any registry implementation must
// provide
type Registry interface {
	StoreArtifact(
		fqn *proto_gen.FullQualifiedName,
		content []byte,
	) (string, error)
	GetArtifact(fqn *proto_gen.FullQualifiedName, hash string) ([]byte, error)
	DeleteArtifact(fqn *proto_gen.FullQualifiedName, hash string) error
}

type Server struct {
	proto_gen.UnimplementedRegistryServiceServer

	registry Registry
}

// NewServer creates a new server with the specified registry implementation
func NewServer(reg Registry) *Server {
	return &Server{
		registry: reg,
	}
}
