package registry

import (
	"artifact-registry/orm"
	"artifact-registry/proto_gen"
	"io"
)

// Registry interface defines the methods that any registry implementation must
// provide
type Registry interface {
	StoreArtifact(
		pkg *proto_gen.PackageName,
		reader io.Reader,
	) (string, error)
	GetArtifact(pkg *proto_gen.PackageName, hash string) ([]byte, error)
	DeleteArtifact(pkg *proto_gen.PackageName, hash string) error
}

var _ proto_gen.RegistryServiceServer = (*Server)(nil)

type Server struct {
	proto_gen.UnimplementedRegistryServiceServer

	registry Registry
	db       orm.DB
}

// NewServer creates a new server with the specified registry implementation
func NewServer(reg Registry, db orm.DB) *Server {
	return &Server{
		registry: reg,
		db:       db,
	}
}
