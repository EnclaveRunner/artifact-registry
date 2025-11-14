package registry

import (
	"artifact-registry/proto_gen"
	"io"
)

// Registry interface defines the methods that any registry implementation must
// provide
type Registry interface {
	StoreArtifact(
		fqn *proto_gen.FullyQualifiedName,
		reader io.Reader,
	) (string, error)
	GetArtifact(fqn *proto_gen.FullyQualifiedName, hash string) ([]byte, error)
	DeleteArtifact(fqn *proto_gen.FullyQualifiedName, hash string) error
}

var _ proto_gen.RegistryServiceServer = (*Server)(nil)

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
