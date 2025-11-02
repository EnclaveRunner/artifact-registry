package procedures

import (
	"artifact-registry/proto_gen"
	"context"

	"github.com/rs/zerolog/log"
)

func (s *Server) QueryArtifacts(
	_ context.Context,
	query *proto_gen.ArtifactQuery,
) (*proto_gen.ArtifactListResponse, error) {
	if s.registry == nil {
		return &proto_gen.ArtifactListResponse{}, nil
	}

	artifacts, err := s.registry.QueryArtifacts(query)
	if err != nil {
		log.Error().Err(err).Msg("Failed to query artifacts")
		return nil, err
	}

	return &proto_gen.ArtifactListResponse{
		Artifacts: artifacts,
	}, nil
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
	_ context.Context,
	req *proto_gen.UploadArtifactRequest,
) (*proto_gen.Artifact, error) {
	log.Info().
		Str("source", req.Fqn.Source).
		Str("author", req.Fqn.Author).
		Str("name", req.Fqn.Name).
		Msg("UploadArtifact triggered")

	if s.registry == nil {
		return &proto_gen.Artifact{}, nil
	}

	// Create artifact from upload request
	artifact := &proto_gen.Artifact{
		Fqn:     req.Fqn,
		Tags:    req.Tags,
		Content: req.Content,
	}

	err := s.registry.StoreArtifact(artifact)
	if err != nil {
		log.Error().Err(err).Msg("Failed to store artifact")
		return nil, err
	}

	// Return artifact without content to reduce response size
	result := *artifact
	result.Content = nil

	return &result, nil
}

func (s *Server) DeleteArtifact(
	_ context.Context,
	id *proto_gen.ArtifactIdentifier,
) (*proto_gen.Artifact, error) {
	log.Info().
		Str("source", id.Fqn.Source).
		Str("author", id.Fqn.Author).
		Str("name", id.Fqn.Name).
		Msg("DeleteArtifact called")

	if s.registry == nil {
		return &proto_gen.Artifact{}, nil
	}

	artifact, err := s.registry.DeleteArtifact(id)
	if err != nil {
		log.Error().Err(err).Msg("Failed to delete artifact")
		return nil, err
	}

	// Return artifact without content to reduce response size
	result := *artifact
	result.Content = nil

	return &result, nil
}

func (s *Server) GetArtifact(
	_ context.Context,
	id *proto_gen.ArtifactIdentifier,
) (*proto_gen.Artifact, error) {
	log.Info().
		Str("source", id.Fqn.Source).
		Str("author", id.Fqn.Author).
		Str("name", id.Fqn.Name).
		Msg("GetArtifact called")

	if s.registry == nil {
		return &proto_gen.Artifact{}, nil
	}

	artifact, err := s.registry.GetArtifact(id)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get artifact")
		return nil, err
	}

	return artifact, nil
}

func (s *Server) AddTag(
	_ context.Context,
	req *proto_gen.AddRemoveTagRequest,
) (*proto_gen.Artifact, error) {
	log.Info().
		Str("source", req.Fqn.Source).
		Str("author", req.Fqn.Author).
		Str("name", req.Fqn.Name).
		Str("tag", req.Tag).
		Msg("AddTag called")

	if s.registry == nil {
		return &proto_gen.Artifact{}, nil
	}

	// Check if the registry supports tag management
	if fsRegistry, ok := s.registry.(interface {
		AddTag(fqn *proto_gen.FullQualifiedName, versionHash, tag string) error
	}); ok {
		err := fsRegistry.AddTag(req.Fqn, req.VersionHash, req.Tag)
		if err != nil {
			log.Error().Err(err).Msg("Failed to add tag")
			return nil, err
		}
	}

	// Return the artifact
	id := &proto_gen.ArtifactIdentifier{
		Fqn: req.Fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: req.VersionHash,
		},
	}

	return s.GetArtifact(context.Background(), id)
}

func (s *Server) RemoveTag(
	_ context.Context,
	req *proto_gen.AddRemoveTagRequest,
) (*proto_gen.Artifact, error) {
	log.Info().
		Str("source", req.Fqn.Source).
		Str("author", req.Fqn.Author).
		Str("name", req.Fqn.Name).
		Str("tag", req.Tag).
		Msg("RemoveTag called")

	if s.registry == nil {
		return &proto_gen.Artifact{}, nil
	}

	// Check if the registry supports tag management
	if fsRegistry, ok := s.registry.(interface {
		RemoveTag(fqn *proto_gen.FullQualifiedName, tag string) error
	}); ok {
		err := fsRegistry.RemoveTag(req.Fqn, req.Tag)
		if err != nil {
			log.Error().Err(err).Msg("Failed to remove tag")
			return nil, err
		}
	}

	// Return the artifact
	id := &proto_gen.ArtifactIdentifier{
		Fqn: req.Fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: req.VersionHash,
		},
	}

	return s.GetArtifact(context.Background(), id)
}
