package registry

import (
	"artifact-registry/orm"
	"artifact-registry/proto_gen"
	"context"
	"errors"
	"io"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const ChunkSize = 1024 * 1024 * 3 // 3MB

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

	fqn := &proto_gen.FullyQualifiedName{}
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
			Fqn: &proto_gen.FullyQualifiedName{
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
	err := validateFQN(req.Fqn)
	if err != nil {
		log.Error().Err(err).Msg("Invalid FQN in PullArtifactRequest")

		return err
	}

	// Validate identifier is not empty
	switch identifier := req.Identifier.(type) {
	case *proto_gen.PullArtifactRequest_VersionHash:
		if identifier.VersionHash == "" {
			log.Error().Msg("Empty versionHash in PullArtifactRequest")

			return &ServiceError{
				Code:    codes.InvalidArgument,
				Message: "versionHash cannot be empty",
				Inner:   ErrEmptyVersionHash,
			}
		}
	case *proto_gen.PullArtifactRequest_Tag:
		if identifier.Tag == "" {
			log.Error().Msg("Empty tag in PullArtifactRequest")

			return &ServiceError{
				Code:    codes.InvalidArgument,
				Message: "tag cannot be empty",
				Inner:   ErrEmptyTag,
			}
		}
	case nil:
		log.Error().Msg("No identifier provided in PullArtifactRequest")

		return newInvalidIdentifierError()
	}

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
	totalSize := len(content)

	log.Info().
		Str("versionHash", versionHash).
		Int("totalSize", totalSize).
		Int("chunkSize", ChunkSize).
		Msg("Starting to stream artifact content")

	for offset := 0; offset < totalSize; offset += ChunkSize {
		end := min(offset+ChunkSize, totalSize)

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
		Int("chunksCount", (totalSize+ChunkSize-1)/ChunkSize).
		Msg("Successfully streamed complete artifact")

	// Increment pull count
	if err := orm.IncreasePullCount(req.Fqn, versionHash); err != nil {
		log.Warn().Err(err).Msg("Failed to increment pull count")
	}

	return nil
}

func (s *Server) UploadArtifact(
	stream grpc.ClientStreamingServer[proto_gen.UploadArtifactRequest, proto_gen.Artifact],
) error {
	firstMessage, err := stream.Recv()
	if err != nil {
		log.Error().Err(err).Msg("Failed to receive first upload artifact message")

		return wrapServiceError(err, "receiving first upload artifact message")
	}

	metadata := firstMessage.GetMetadata()
	if metadata == nil {
		log.Error().Msg("UploadArtifactRequest missing metadata")

		return &ServiceError{
			Code:    codes.InvalidArgument,
			Message: "Expected first message to be metadata",
		}
	}

	err = validateFQN(metadata.Fqn)
	if err != nil {
		log.Error().Err(err).Msg("Invalid FQN in UploadArtifactRequest metadata")

		return err
	}

	log.Info().
		Str("source", metadata.Fqn.Source).
		Str("author", metadata.Fqn.Author).
		Str("name", metadata.Fqn.Name).
		Msg("UploadArtifact triggered")

	if s.registry == nil {
		return newRegistryUnavailableError("artifact upload")
	}

	pr, pw := io.Pipe()

	resultChan := make(chan struct {
		versionHash string
		err         error
	}, 1)

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	go func() {
		defer pr.Close()
		versionHash, err := s.registry.StoreArtifact(metadata.Fqn, pr)
		select {
		case resultChan <- struct {
			versionHash string
			err         error
		}{versionHash, err}:
		case <-ctx.Done():
		}
		close(resultChan)
	defer func() { <-resultChan }()

	for {
		message, err := stream.Recv()
		log.Debug().Msg("Read chunk from upload stream")
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			log.Error().Err(err).Msg("Failed to receive upload artifact message")
			_ = pw.CloseWithError(err)

			return wrapServiceError(err, "receiving upload artifact message")
		}

		chunk := message.GetContent()
		if chunk == nil {
			_ = pw.CloseWithError(errors.New("missing content chunk"))
			}

			return &ServiceError{
				Code:    codes.InvalidArgument,
				Message: "Expected content chunk in upload artifact message",
			}
		}

		_, err = pw.Write(chunk.Data)
		if err != nil {
			_ = pw.CloseWithError(err)
			log.Error().Msgf("Error writing chunk to writer: %v", err)
		}
	}

	// Close the writer when done
	err = pw.Close()
	if err != nil {
		log.Error().Err(err).Msg("Failed to close artifact content writer")

		return wrapServiceError(err, "closing artifact content writer")
	}

	result, ok := <-resultChan
		if ctxErr := stream.Context().Err(); ctxErr != nil {
			return wrapServiceError(ctxErr, "artifact upload cancelled")
		}
		return wrapServiceError(errors.New("unexpected channel close"), "artifact upload")
		return wrapServiceError(context.Canceled, "artifact upload cancelled")
	}
	versionHash := result.versionHash
	err = result.err
	if err != nil {
		log.Error().Err(err).Msg("Failed to store artifact")

		return wrapServiceError(err, "storing artifact")
	}

	err = orm.StoreArtifactMeta(metadata.Fqn, versionHash)
	if err != nil {
		log.Error().Err(err).Msg("Failed to store artifact metadata")
		_ = s.registry.DeleteArtifact(metadata.Fqn, versionHash)

		return wrapServiceError(err, "storing artifact metadata")
	}

	// Add tags to the artifact
	for _, tag := range metadata.Tags {
		err = orm.AddTag(stream.Context(), metadata.Fqn, versionHash, tag)
		if err != nil {
			log.Error().
				Err(err).
				Str("tag", tag).
				Msg("Failed to add tag to artifact")

			return wrapServiceError(err, "adding tag to artifact")
		}
	}

	artifact, err := orm.GetArtifactMetaByHash(
		stream.Context(),
		metadata.Fqn,
		versionHash,
	)
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve stored artifact metadata")

		return wrapServiceError(err, "retrieving stored artifact metadata")
	}

	log.Info().
		Str("source", artifact.Source).
		Str("author", artifact.Author).
		Str("name", artifact.Name).
		Str("versionHash", versionHash).
		Msg("Artifact uploaded successfully")

	err = stream.SendAndClose(&proto_gen.Artifact{
		Fqn:         metadata.Fqn,
		VersionHash: versionHash,
		Tags:        metadata.Tags,
		Metadata: &proto_gen.MetaData{
			Created: timestamppb.New(artifact.CreatedAt),
			Pulls:   artifact.PullsCount,
		},
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to send upload artifact response")

		return wrapServiceError(err, "sending upload artifact response")
	}

	return nil
}

func (s *Server) DeleteArtifact(
	ctx context.Context,
	id *proto_gen.ArtifactIdentifier,
) (*proto_gen.Artifact, error) {
	err := validateFQN(id.Fqn)
	if err != nil {
		log.Error().Err(err).Msg("Invalid FQN in DeleteArtifact request")

		return nil, err
	}

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
		Fqn: &proto_gen.FullyQualifiedName{
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
	err := validateFQN(id.Fqn)
	if err != nil {
		log.Error().Err(err).Msg("Invalid FQN in GetArtifact request")

		return nil, err
	}

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
		Fqn: &proto_gen.FullyQualifiedName{
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
	err := validateAddRemoveTagRequest(req)
	if err != nil {
		log.Error().Err(err).Msg("Invalid AddTag request")

		return nil, err
	}

	log.Info().
		Str("source", req.Fqn.Source).
		Str("author", req.Fqn.Author).
		Str("name", req.Fqn.Name).
		Str("tag", req.Tag).
		Msg("Tag creation requested")

	if s.registry == nil {
		return nil, newRegistryUnavailableError("adding tag")
	}

	err = orm.AddTag(ctx, req.Fqn, req.VersionHash, req.Tag)
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
	err := validateAddRemoveTagRequest(req)
	if err != nil {
		log.Error().Err(err).Msg("Invalid RemoveTag request")

		return nil, err
	}

	log.Info().
		Str("source", req.Fqn.Source).
		Str("author", req.Fqn.Author).
		Str("name", req.Fqn.Name).
		Str("tag", req.Tag).
		Msg("RemoveTag called")

	if s.registry == nil {
		return nil, newRegistryUnavailableError("removing tag")
	}

	err = orm.RemoveTag(ctx, req.Fqn, req.Tag)
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
	err := validateArtifactIdentifier(id)
	if err != nil {
		log.Error().Err(err).Msg("Invalid artifact identifier")

		return nil, err
	}

	var artifactMeta *orm.Artifact
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
