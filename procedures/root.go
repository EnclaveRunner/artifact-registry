package procedures

import (
	proto "artifact-registry/proto_gen"
)

type Server struct {
	proto.UnimplementedRegistryServiceServer
}
