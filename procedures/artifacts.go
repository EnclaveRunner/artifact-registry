package procedures

import (
	"artifact-registry/proto_gen"
	"context"

	"github.com/rs/zerolog/log"
)

func (s *Server) QueryArtifacts(
	context.Context,
	*proto_gen.ArtifactQuery,
) (*proto_gen.ArtifactListResponse, error) {
	// TODO: unimplemented
	log.Info().Msg("QueryArtifacts called - unimplemented")

	return &proto_gen.ArtifactListResponse{}, nil
}

func (s *Server) PullArtifact(
	*proto_gen.PullArtifactRequest,
	proto_gen.RegistryService_PullArtifactServer,
) error {
	// TODO: unimplemented
	log.Info().Msg("PullArtifact called - unimplemented")

	return nil
}

func (s *Server) UploadArtifact(
	context.Context,
	*proto_gen.UploadArtifactRequest,
) (*proto_gen.Artifact, error) {
	// TODO: unimplemented
	log.Info().Msg("UploadArtifact called - unimplemented")

	return &proto_gen.Artifact{}, nil
}

func (s *Server) DeleteArtifact(
	context.Context,
	*proto_gen.ArtifactIdentifier,
) (*proto_gen.Artifact, error) {
	// TODO: unimplemented
	log.Info().Msg("DeleteArtifact called - unimplemented")

	return &proto_gen.Artifact{}, nil
}

func (s *Server) GetArtifact(
	context.Context,
	*proto_gen.ArtifactIdentifier,
) (*proto_gen.Artifact, error) {
	// TODO: unimplemented
	log.Info().Msg("GetArtifact called - unimplemented")

	return &proto_gen.Artifact{}, nil
}

func (s *Server) AddTag(
	context.Context,
	*proto_gen.AddRemoveTagRequest,
) (*proto_gen.Artifact, error) {
	// TODO: unimplemented
	log.Info().Msg("AddTag called - unimplemented")

	return &proto_gen.Artifact{}, nil
}

func (s *Server) RemoveTag(
	context.Context,
	*proto_gen.AddRemoveTagRequest,
) (*proto_gen.Artifact, error) {
	// TODO: unimplemented
	log.Info().Msg("RemoveTag called - unimplemented")

	return &proto_gen.Artifact{}, nil
}
