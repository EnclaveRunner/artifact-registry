package registry

import (
	"artifact-registry/orm"
	"artifact-registry/proto_gen"
	"context"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) QueryArtifacts(
	ctx context.Context,
	query *proto_gen.ArtifactQuery,
) (*proto_gen.ArtifactListResponse, error) {
	logEvent := log.Info()
	if query.Source != nil {
		logEvent = logEvent.Str("source", *query.Source)
	}
	if query.Author != nil {
		logEvent = logEvent.Str("author", *query.Author)
	}
	if query.Name != nil {
		logEvent = logEvent.Str("name", *query.Name)
	}
	logEvent.Msg("Artifacts queried with FQN query")

	if s.registry == nil {
		return &proto_gen.ArtifactListResponse{}, nil
	}

	fqn := &proto_gen.FullQualifiedName{}
	if query.Source != nil {
		fqn.Source = *query.Source
	}
	if query.Author != nil {
		fqn.Author = *query.Author
	}
	if query.Name != nil {
		fqn.Name = *query.Name
	}

	artifacts, err := orm.GetArtifactMetasByFQN(ctx, fqn)
	if err != nil {
		log.Error().Err(err).Msg("Failed to query artifacts")

		return nil, wrapServiceError(err, "querying artifacts")
	}

	// Convert []orm.Artifact to []*proto_gen.Artifact
	protoArtifacts := make([]*proto_gen.Artifact, 0, len(artifacts))
	for _, a := range artifacts {
		protoArtifacts = append(protoArtifacts, &proto_gen.Artifact{
			Fqn: &proto_gen.FullQualifiedName{
				Source: a.Source,
				Author: a.Author,
				Name:   a.Name,
			},
			VersionHash: a.Hash,
			Tags:        tagsToStrings(a.Tags),
			Metadata: &proto_gen.MetaData{
				Created: timestamppb.New(a.CreatedAt),
				Pulls:   a.PullsCount,
			},
		})
	}

	return &proto_gen.ArtifactListResponse{
		Artifacts: protoArtifacts,
	}, nil
}

func (s *Server) PullArtifact(
	req *proto_gen.PullArtifactRequest,
	serv proto_gen.RegistryService_PullArtifactServer,
) error {
	log.Info().
		Str("source", req.Fqn.Source).
		Str("author", req.Fqn.Author).
		Str("name", req.Fqn.Name).
		Msg("Artifact pull requested")

	if s.registry == nil {
		log.Error().Msg("Registry is nil")

		return newRegistryUnavailableError("artifact pull")
	}

	var artifactMeta *orm.Artifact
	var err error

	switch identifier := req.Identifier.(type) {
	case *proto_gen.PullArtifactRequest_VersionHash:
		artifactMeta, err = orm.GetArtifactMetaByHash(serv.Context(), req.Fqn, identifier.VersionHash)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get artifact by version hash")

			return wrapServiceError(err, "retrieving artifact by version hash")
		}
	case *proto_gen.PullArtifactRequest_Tag:
		artifactMeta, err = orm.GetArtifactMetaByTag(serv.Context(), req.Fqn, identifier.Tag)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get artifact by tag")

			return wrapServiceError(err, "retrieving artifact by tag")
		}
	default:
		log.Error().Msg("No valid identifier provided in PullArtifactRequest")

		return newInvalidIdentifierError()
	}

	// Get the artifact from the registry
	content, err := s.registry.GetArtifact(req.Fqn, artifactMeta.Hash)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get artifact for pull")

		return wrapServiceError(err, "retrieving artifact content")
	}

	// Update versionHash with actual hash from artifact
	versionHash := artifactMeta.Hash

	// Stream the artifact content back to the client in chunks
	const chunkSize = 64 * 1024 // 64KB chunks
	totalSize := len(content)

	log.Info().
		Str("versionHash", versionHash).
		Int("totalSize", totalSize).
		Int("chunkSize", chunkSize).
		Msg("Starting to stream artifact content")

	for offset := 0; offset < totalSize; offset += chunkSize {
		end := offset + chunkSize
		if end > totalSize {
			end = totalSize
		}

		chunk := content[offset:end]
		response := &proto_gen.ArtifactContent{
			Data: chunk,
		}

		if err := serv.Send(response); err != nil {
			log.Error().
				Err(err).
				Int("offset", offset).
				Int("chunkSize", len(chunk)).
				Msg("Failed to send artifact content chunk")

			return wrapServiceError(err, "streaming artifact content")
		}

		log.Debug().
			Int("offset", offset).
			Int("chunkSize", len(chunk)).
			Int("totalSize", totalSize).
			Msg("Sent artifact content chunk")
	}

	log.Info().
		Str("versionHash", versionHash).
		Int("totalSize", totalSize).
		Int("chunksCount", (totalSize+chunkSize-1)/chunkSize).
		Msg("Successfully streamed complete artifact")

	// Increment pull count
	if err := orm.IncreasePullCount(req.Fqn, versionHash); err != nil {
		log.Warn().Err(err).Msg("Failed to increment pull count")
	}

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
		return nil, newRegistryUnavailableError("artifact upload")
	}

	versionHash, err := s.registry.StoreArtifact(req.Fqn, req.Content)
	if err != nil {
		log.Error().Err(err).Msg("Failed to store artifact")

		return nil, wrapServiceError(err, "storing artifact")
	}

	return &proto_gen.Artifact{
		Fqn:         req.Fqn,
		VersionHash: versionHash,
		Tags:        req.Tags,
	}, nil
}

func (s *Server) DeleteArtifact(
	ctx context.Context,
	id *proto_gen.ArtifactIdentifier,
) (*proto_gen.Artifact, error) {
	log.Info().
		Str("source", id.Fqn.Source).
		Str("author", id.Fqn.Author).
		Str("name", id.Fqn.Name).
		Msg("Deletion of artifact requested")

	if s.registry == nil {
		return nil, newRegistryUnavailableError("artifact deletion")
	}

	artifactMeta, err := resolveIdentifier(ctx, id)
	if err != nil {
		log.Error().Err(err).Msg("Failed to resolve identifier to hash")

		return nil, wrapServiceError(
			err,
			"resolving artifact identifier for deletion",
		)
	}

	err = s.registry.DeleteArtifact(id.Fqn, artifactMeta.Hash)
	if err != nil {
		log.Error().Err(err).Msg("Failed to delete artifact")

		return nil, wrapServiceError(err, "deleting artifact")
	}

	result := &proto_gen.Artifact{
		Fqn: &proto_gen.FullQualifiedName{
			Source: artifactMeta.Source,
			Author: artifactMeta.Author,
			Name:   artifactMeta.Name,
		},
		VersionHash: artifactMeta.Hash,
		Tags:        tagsToStrings(artifactMeta.Tags),
		Metadata: &proto_gen.MetaData{
			Created: timestamppb.New(artifactMeta.CreatedAt),
			Pulls:   artifactMeta.PullsCount,
		},
	}

	return result, nil
}

func (s *Server) GetArtifact(
	ctx context.Context,
	id *proto_gen.ArtifactIdentifier,
) (*proto_gen.Artifact, error) {
	log.Info().
		Str("source", id.Fqn.Source).
		Str("author", id.Fqn.Author).
		Str("name", id.Fqn.Name).
		Msg("Information about an artifact requested")

	if s.registry == nil {
		return nil, newRegistryUnavailableError("artifact retrieval")
	}

	artifactMeta, err := resolveIdentifier(ctx, id)
	if err != nil {
		log.Error().Err(err).Msg("Failed to resolve identifier to hash")

		return nil, err // Already wrapped by resolveIdentifier
	}

	return &proto_gen.Artifact{
		Fqn: &proto_gen.FullQualifiedName{
			Source: artifactMeta.Source,
			Author: artifactMeta.Author,
			Name:   artifactMeta.Name,
		},
		VersionHash: artifactMeta.Hash,
		Tags:        tagsToStrings(artifactMeta.Tags),
		Metadata: &proto_gen.MetaData{
			Created: timestamppb.New(artifactMeta.CreatedAt),
			Pulls:   artifactMeta.PullsCount,
		},
	}, nil
}

func (s *Server) AddTag(
	ctx context.Context,
	req *proto_gen.AddRemoveTagRequest,
) (*proto_gen.Artifact, error) {
	log.Info().
		Str("source", req.Fqn.Source).
		Str("author", req.Fqn.Author).
		Str("name", req.Fqn.Name).
		Str("tag", req.Tag).
		Msg("Tag creation requested")

	if s.registry == nil {
		return nil, newRegistryUnavailableError("adding tag")
	}

	err := orm.AddTag(ctx, req.Fqn, req.VersionHash, req.Tag)
	if err != nil {
		log.Error().Err(err).Msg("Failed to add tag")

		return nil, wrapServiceError(err, "adding tag to artifact")
	}

	// Return the artifact
	id := &proto_gen.ArtifactIdentifier{
		Fqn: req.Fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: req.VersionHash,
		},
	}

	return s.GetArtifact(ctx, id)
}

func (s *Server) RemoveTag(
	ctx context.Context,
	req *proto_gen.AddRemoveTagRequest,
) (*proto_gen.Artifact, error) {
	log.Info().
		Str("source", req.Fqn.Source).
		Str("author", req.Fqn.Author).
		Str("name", req.Fqn.Name).
		Str("tag", req.Tag).
		Msg("RemoveTag called")

	if s.registry == nil {
		return nil, newRegistryUnavailableError("removing tag")
	}

	err := orm.RemoveTag(ctx, req.Fqn, req.Tag)
	if err != nil {
		log.Error().Err(err).Msg("Failed to remove tag")

		return nil, wrapServiceError(err, "removing tag from artifact")
	}

	// Return the artifact
	id := &proto_gen.ArtifactIdentifier{
		Fqn: req.Fqn,
		Identifier: &proto_gen.ArtifactIdentifier_VersionHash{
			VersionHash: req.VersionHash,
		},
	}

	return s.GetArtifact(ctx, id)
}

func resolveIdentifier(
	ctx context.Context,
	id *proto_gen.ArtifactIdentifier,
) (*orm.Artifact, error) {
	var artifactMeta *orm.Artifact
	var err error
	switch identifier := id.Identifier.(type) {
	case *proto_gen.ArtifactIdentifier_VersionHash:
		artifactMeta, err = orm.GetArtifactMetaByHash(ctx, id.Fqn, identifier.VersionHash)
		if err != nil {
			log.Error().Err(err).Msg("Failed to resolve artifact by version hash")

			return nil, wrapServiceError(err, "resolving artifact by version hash")
		}
	case *proto_gen.ArtifactIdentifier_Tag:
		artifactMeta, err = orm.GetArtifactMetaByTag(ctx, id.Fqn, identifier.Tag)
		if err != nil {
			log.Error().Err(err).Msg("Failed to resolve artifact by version tag")

			return nil, wrapServiceError(err, "resolving artifact by tag")
		}
	default:
		log.Error().Msg("No valid identifier provided")

		return nil, newInvalidIdentifierError()
	}

	return artifactMeta, nil
}

func tagsToStrings(tags []orm.Tag) []string {
	resultTags := make([]string, 0, len(tags))
	for _, t := range tags {
		resultTags = append(resultTags, t.TagName)
	}

	return resultTags
}
