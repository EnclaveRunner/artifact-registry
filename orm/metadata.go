package orm

import (
	"artifact-registry/proto_gen"
	"context"
	"fmt"

	"gorm.io/gorm"
)

func GetArtifactMetaByHash(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
	hash string,
) (*Artifact, error) {
	var artifact Artifact

	artifact, err := gorm.G[Artifact](
		DB,
	).Where(&Artifact{
		Source: fqn.Source,
		Author: fqn.Author,
		Name:   fqn.Name,
		Hash:   hash,
	}).First(ctx)
	if err != nil {
		return nil, wrapErrorWithDetails(
			err,
			"get artifact by hash",
			fmt.Sprintf(
				"source=%s, author=%s, name=%s, hash=%s",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				hash,
			),
		)
	}

	return &artifact, nil
}

func GetArtifactMetaByTag(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
	tag string,
) (*Artifact, error) {
	var artifacts []Artifact

	artifacts, err := gorm.G[Artifact](
		DB,
	).Where(&Artifact{
		Source: fqn.Source,
		Author: fqn.Author,
		Name:   fqn.Name,
	}).Find(ctx)
	if err != nil {
		return nil, wrapErrorWithDetails(
			err,
			"get artifact by tag",
			fmt.Sprintf(
				"source=%s, author=%s, name=%s, tag=%s",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				tag,
			),
		)
	}

	for _, artifact := range artifacts {
		if hasTag(&artifact, tag) {
			return &artifact, nil
		}
	}

	return nil, &NotFoundError{
		Search: fmt.Sprintf(
			"artifact with tag=%s for source=%s, author=%s, name=%s",
			tag,
			fqn.Source,
			fqn.Author,
			fqn.Name,
		),
	}
}

func hasTag(artifact *Artifact, tag string) bool {
	for _, t := range artifact.Tags {
		if t.TagName == tag {
			return true
		}
	}

	return false
}

func IncreasePullCount(fqn *proto_gen.FullyQualifiedName, hash string) error {
	var artifact Artifact

	artifact, err := gorm.G[Artifact](
		DB,
	).Where(&Artifact{
		Source: fqn.Source,
		Author: fqn.Author,
		Name:   fqn.Name,
		Hash:   hash,
	}).First(context.Background())
	if err != nil {
		return wrapErrorWithDetails(
			err,
			"increase pull count - find artifact",
			fmt.Sprintf(
				"source=%s, author=%s, name=%s, hash=%s",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				hash,
			),
		)
	}

	artifact.PullsCount += 1

	return wrapErrorWithDetails(
		DB.Save(&artifact).Error,
		"increase pull count - save artifact",
		fmt.Sprintf(
			"source=%s, author=%s, name=%s, hash=%s",
			fqn.Source,
			fqn.Author,
			fqn.Name,
			hash,
		),
	)
}

func GetArtifactMetasByFQN(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
) ([]Artifact, error) {
	var artifacts []Artifact

	artifacts, err := gorm.G[Artifact](
		DB,
	).Where(&Artifact{
		Source: fqn.Source,
		Author: fqn.Author,
		Name:   fqn.Name,
	}).Find(ctx)
	if err != nil {
		return nil, wrapErrorWithDetails(
			err,
			"get artifacts by FQN",
			fmt.Sprintf(
				"source=%s, author=%s, name=%s",
				fqn.Source,
				fqn.Author,
				fqn.Name,
			),
		)
	}

	return artifacts, nil
}

func StoreArtifactMeta(
	fqn *proto_gen.FullyQualifiedName,
	versionHash string,
) error {
	return wrapErrorWithDetails(
		DB.Save(
			&Artifact{
				Source: fqn.Source,
				Author: fqn.Author,
				Name:   fqn.Name,
				Hash:   versionHash,
			},
		).Error,
		"store artifact metadata",
		fmt.Sprintf(
			"source=%s, author=%s, name=%s, hash=%s",
			fqn.Source,
			fqn.Author,
			fqn.Name,
			versionHash,
		),
	)
}

func DeleteArtifactMeta(
	fqn *proto_gen.FullyQualifiedName,
	versionHash string,
) error {
	return wrapErrorWithDetails(
		DB.Delete(
			&Artifact{
				Source: fqn.Source,
				Author: fqn.Author,
				Name:   fqn.Name,
				Hash:   versionHash,
			},
		).Error,
		"delete artifact metadata",
		fmt.Sprintf(
			"source=%s, author=%s, name=%s, hash=%s",
			fqn.Source,
			fqn.Author,
			fqn.Name,
			versionHash,
		),
	)
}

func AddTag(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
	versionHash, tag string,
) error {
	var artifact Artifact

	artifact, err := gorm.G[Artifact](
		DB,
	).Where(&Artifact{
		Source: fqn.Source,
		Author: fqn.Author,
		Name:   fqn.Name,
		Hash:   versionHash,
	}).First(ctx)
	if err != nil {
		return wrapErrorWithDetails(
			err,
			"add tag - find artifact",
			fmt.Sprintf(
				"source=%s, author=%s, name=%s, hash=%s, tag=%s",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				versionHash,
				tag,
			),
		)
	}

	// Check if tag already exists
	for _, t := range artifact.Tags {
		if t.TagName == tag {
			return nil // Tag already exists, nothing to do
		}
	}

	// Add new tag
	artifact.Tags = append(artifact.Tags, Tag{TagName: tag})

	return wrapErrorWithDetails(
		DB.Save(&artifact).Error,
		"add tag - save artifact",
		fmt.Sprintf(
			"source=%s, author=%s, name=%s, hash=%s, tag=%s",
			fqn.Source,
			fqn.Author,
			fqn.Name,
			versionHash,
			tag,
		),
	)
}

func RemoveTag(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
	tag string,
) error {
	var artifact Artifact

	artifact, err := gorm.G[Artifact](
		DB,
	).Where(&Artifact{
		Source: fqn.Source,
		Author: fqn.Author,
		Name:   fqn.Name,
	}).First(ctx)
	if err != nil {
		return wrapErrorWithDetails(
			err,
			"remove tag - find artifact",
			fmt.Sprintf(
				"source=%s, author=%s, name=%s, tag=%s",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				tag,
			),
		)
	}

	// Remove tag
	newTags := make([]Tag, 0)
	for _, t := range artifact.Tags {
		if t.TagName != tag {
			newTags = append(newTags, t)
		}
	}
	artifact.Tags = newTags

	return wrapErrorWithDetails(
		DB.Save(&artifact).Error,
		"remove tag - save artifact",
		fmt.Sprintf(
			"source=%s, author=%s, name=%s, tag=%s",
			fqn.Source,
			fqn.Author,
			fqn.Name,
			tag,
		),
	)
}
