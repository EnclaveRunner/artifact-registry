package orm

import (
	"artifact-registry/proto_gen"
	"context"

	"gorm.io/gorm"
)

func GetArtifactMetaByHash(fqn *proto_gen.FullQualifiedName, hash string) (*Artifact, error) {
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
		return nil, err
	}
	increasePullCount(fqn, hash)
	return &artifact, nil
}

func GetArtifactMetaByTag(fqn *proto_gen.FullQualifiedName, tag string) (*Artifact, error) {
	var artifacts []Artifact

	artifacts, err := gorm.G[Artifact](
		DB,
	).Where(&Artifact{
		Source: fqn.Source,
		Author: fqn.Author,
		Name:   fqn.Name,
	}).Find(context.Background())
	if err != nil {
		return nil, err
	}

	for _, artifact := range artifacts {
		if hasTag(artifact, tag) {
			increasePullCount(fqn, artifact.Hash)
			return &artifact, nil
		}
	}
	
	return nil, gorm.ErrRecordNotFound
}

func hasTag(artifact Artifact, tag string) bool {
	for _, t := range artifact.Tags {
		if t.TagName == tag {
			return true
		}
	}
	return false
}

func increasePullCount(fqn *proto_gen.FullQualifiedName, hash string) error {
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
		return err
	}
	
	artifact.PullsCount += 1
	return DB.Save(&artifact).Error
}

func StoreArtifactMeta(fqn *proto_gen.FullQualifiedName, versionHash string) error {
	return DB.Save(
		&Artifact{
			Source: fqn.Source,
			Author: fqn.Author,
			Name:   fqn.Name,
			Hash:   versionHash,
		},
	).Error
}

func DeleteArtifactMeta(fqn *proto_gen.FullQualifiedName, versionHash string) error {
	return DB.Delete(
		&Artifact{
			Source: fqn.Source,
			Author: fqn.Author,
			Name:   fqn.Name,
			Hash:   versionHash,
		},
	).Error
}