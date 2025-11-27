package orm

import (
	"artifact-registry/proto_gen"
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

func (db *DB) GetArtifactMetaByHash(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
	hash string,
) (*Artifact, error) {
	if fqn == nil {
		return nil, &BadInputError{
			Reason: "artifact with nil FullyQualifiedName",
		}
	}

	if hash == "" || fqn.Source == "" || fqn.Author == "" || fqn.Name == "" {
		return nil, &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: source=%q, author=%q, name=%q, hash=%q",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				hash,
			),
		}
	}

	var artifact Artifact

	artifact, err := gorm.G[Artifact](
		db.dbGorm,
	).Preload("Tags", nil).Where(&Artifact{
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

func (db *DB) GetArtifactMetaByTag(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
	tag string,
) (*Artifact, error) {
	if fqn == nil {
		return nil, &BadInputError{
			Reason: "artifact with nil FullyQualifiedName",
		}
	}

	if tag == "" || fqn.Source == "" || fqn.Author == "" || fqn.Name == "" {
		return nil, &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: source=%q, author=%q, name=%q, tag=%q",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				tag,
			),
		}
	}

	tagQuery, err := gorm.G[Tag](db.dbGorm).Where(&Tag{
		Source:  fqn.Source,
		Author:  fqn.Author,
		Name:    fqn.Name,
		TagName: tag,
	}).First(ctx)
	if err != nil {
		return nil, wrapErrorWithDetails(
			err,
			"get tag",
			fmt.Sprintf(
				"source=%s, author=%s, name=%s, tag=%s",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				tag,
			),
		)
	}

	return db.GetArtifactMetaByHash(ctx, fqn, tagQuery.Hash)
}

func (db *DB) IncreasePullCount(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
	hash string,
) error {
	artifact, err := db.GetArtifactMetaByHash(ctx, fqn, hash)
	if err != nil {
		return err
	}

	artifact.PullsCount += 1

	return wrapErrorWithDetails(
		db.dbGorm.Save(&artifact).Error,
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

func (db *DB) GetArtifactMetasByFQN(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
) ([]Artifact, error) {
	if fqn == nil {
		return nil, &BadInputError{
			Reason: "artifact with nil FullyQualifiedName",
		}
	}

	artifacts, err := gorm.G[Artifact](
		db.dbGorm,
	).Preload("Tags", nil).Where(&Artifact{
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

func (db *DB) CreateArtifactMeta(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
	versionHash string,
	tags ...string,
) error {
	if fqn == nil {
		return &BadInputError{
			Reason: "artifact with nil FullyQualifiedName",
		}
	}

	if versionHash == "" || fqn.Source == "" || fqn.Author == "" ||
		fqn.Name == "" {
		return &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: source=%q, author=%q, name=%q, hash=%q",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				versionHash,
			),
		}
	}

	detailString := fmt.Sprintf(
		"source=%q, author=%q, name=%q, hash=%q, tags=%v",
		fqn.Source,
		fqn.Author,
		fqn.Name,
		versionHash,
		tags,
	)

	err := db.dbGorm.Transaction(func(tx *gorm.DB) error {
		dbTx := db.UseTransaction(tx)
		err := gorm.G[Artifact](tx).Create(ctx, &Artifact{
			Source: fqn.Source,
			Author: fqn.Author,
			Name:   fqn.Name,
			Hash:   versionHash,
		})
		if err != nil {
			return wrapErrorWithDetails(
				err,
				"create artifact metadata",
				detailString,
			)
		}

		for _, tag := range tags {
			err := dbTx.addTag(ctx, fqn, versionHash, tag)
			if err != nil {
				return err
			}
		}

		return nil
	})

	//nolint:wrapcheck // Error already wrapped
	return err
}

func (db *DB) DeleteArtifactMeta(
	fqn *proto_gen.FullyQualifiedName,
	versionHash string,
) error {
	if fqn == nil {
		return &BadInputError{
			Reason: "artifact with nil FullyQualifiedName",
		}
	}

	if versionHash == "" || fqn.Source == "" || fqn.Author == "" ||
		fqn.Name == "" {
		return &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: source=%q, author=%q, name=%q, hash=%q",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				versionHash,
			),
		}
	}

	return wrapErrorWithDetails(
		db.dbGorm.Delete(
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

func (db *DB) AddTag(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
	versionHash, tag string,
) error {
	if fqn == nil {
		return &BadInputError{
			Reason: "artifact with nil FullyQualifiedName",
		}
	}

	if versionHash == "" || tag == "" || fqn.Source == "" || fqn.Author == "" ||
		fqn.Name == "" {
		return &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: source=%q, author=%q, name=%q, hash=%q, tag=%q",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				versionHash,
				tag,
			),
		}
	}

	detailString := fmt.Sprintf(
		"source=%q, author=%q, name=%q, hash=%q, tag=%q",
		fqn.Source,
		fqn.Author,
		fqn.Name,
		versionHash,
		tag,
	)

	// Check that artifact exists
	count, err := gorm.G[Artifact](db.dbGorm).Where(Artifact{
		Source: fqn.Source,
		Author: fqn.Author,
		Name:   fqn.Name,
		Hash:   versionHash,
	}).Count(ctx, "*")
	if err != nil {
		return wrapErrorWithDetails(
			err,
			"check artifact exists",
			detailString,
		)
	}

	if count == 0 {
		return &NotFoundError{
			Search: fmt.Sprintf(
				"Artifact source=%q, author=%q, name=%q, versionHash=%q does not exist",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				versionHash,
			),
		}
	}

	return db.addTag(ctx, fqn, versionHash, tag)
}

func (db *DB) addTag(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
	versionHash, tag string,
) error {
	tagObject := Tag{
		Source:  fqn.Source,
		Author:  fqn.Author,
		Name:    fqn.Name,
		TagName: tag,
	}

	detailString := fmt.Sprintf(
		"source=%q, author=%q, name=%q, hash=%q, tag=%q",
		fqn.Source,
		fqn.Author,
		fqn.Name,
		versionHash,
		tag,
	)

	// Delete existing tag if it exists
	_, err := gorm.G[Tag](db.dbGorm).Where(&tagObject).Delete(ctx)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return wrapErrorWithDetails(
			err,
			"delete existing tag",
			detailString,
		)
	}

	tagObject.Hash = versionHash

	err = gorm.G[Tag](db.dbGorm).Create(ctx, &tagObject)
	if err != nil {
		return wrapErrorWithDetails(
			err,
			"create tag",
			detailString,
		)
	}

	return nil
}

func (db *DB) RemoveTag(
	ctx context.Context,
	fqn *proto_gen.FullyQualifiedName,
	tag string,
) error {
	if fqn == nil {
		return &BadInputError{
			Reason: "artifact with nil FullyQualifiedName",
		}
	}

	if tag == "" || fqn.Source == "" || fqn.Author == "" || fqn.Name == "" {
		return &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: source=%q, author=%q, name=%q, tag=%q",
				fqn.Source,
				fqn.Author,
				fqn.Name,
				tag,
			),
		}
	}

	return wrapErrorWithDetails(
		db.dbGorm.Delete(Tag{
			Source:  fqn.Source,
			Author:  fqn.Author,
			Name:    fqn.Name,
			TagName: tag,
		}).Error,
		"delete tag",
		fmt.Sprintf(
			"source=%q, author=%q, name=%q, tag=%q",
			fqn.Source,
			fqn.Author,
			fqn.Name,
			tag,
		),
	)
}
