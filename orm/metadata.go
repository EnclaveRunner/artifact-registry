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
	pkg *proto_gen.PackageName,
	hash string,
) (*Artifact, error) {
	if pkg == nil {
		return nil, &BadInputError{
			Reason: "artifact with nil PackageName",
		}
	}

	if hash == "" || pkg.Namespace == "" || pkg.Name == "" {
		return nil, &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: namespace=%q, name=%q, hash=%q",
				pkg.Namespace,
				pkg.Name,
				hash,
			),
		}
	}

	var artifact Artifact

	artifact, err := gorm.G[Artifact](
		db.dbGorm,
	).Preload("Tags", nil).Where(&Artifact{
		Author: pkg.Namespace,
		Name:   pkg.Name,
		Hash:   hash,
	}).First(ctx)
	if err != nil {
		return nil, wrapErrorWithDetails(
			err,
			"get artifact by hash",
			fmt.Sprintf(
				"namespace=%s, name=%s, hash=%s",
				pkg.Namespace,
				pkg.Name,
				hash,
			),
		)
	}

	return &artifact, nil
}

func (db *DB) GetArtifactMetaByTag(
	ctx context.Context,
	pkg *proto_gen.PackageName,
	tag string,
) (*Artifact, error) {
	if pkg == nil {
		return nil, &BadInputError{
			Reason: "artifact with nil PackageName",
		}
	}

	if tag == "" || pkg.Namespace == "" || pkg.Name == "" {
		return nil, &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: namespace=%q, name=%q, tag=%q",
				pkg.Namespace,
				pkg.Name,
				tag,
			),
		}
	}

	tagQuery, err := gorm.G[Tag](db.dbGorm).Where(&Tag{
		Author:  pkg.Namespace,
		Name:    pkg.Name,
		TagName: tag,
	}).First(ctx)
	if err != nil {
		return nil, wrapErrorWithDetails(
			err,
			"get tag",
			fmt.Sprintf(
				"namespace=%s, name=%s, tag=%s",
				pkg.Namespace,
				pkg.Name,
				tag,
			),
		)
	}

	return db.GetArtifactMetaByHash(ctx, pkg, tagQuery.Hash)
}

func (db *DB) IncreasePullCount(
	ctx context.Context,
	pkg *proto_gen.PackageName,
	hash string,
) error {
	artifact, err := db.GetArtifactMetaByHash(ctx, pkg, hash)
	if err != nil {
		return err
	}

	artifact.PullsCount += 1

	return wrapErrorWithDetails(
		db.dbGorm.Save(&artifact).Error,
		"increase pull count - save artifact",
		fmt.Sprintf(
			"namespace=%s, name=%s, hash=%s",
			pkg.Namespace,
			pkg.Name,
			hash,
		),
	)
}

func (db *DB) GetArtifactMetasByFQN(
	ctx context.Context,
	pkg *proto_gen.PackageName,
) ([]Artifact, error) {
	if pkg == nil {
		return nil, &BadInputError{
			Reason: "artifact with nil PackageName",
		}
	}

	artifacts, err := gorm.G[Artifact](
		db.dbGorm,
	).Preload("Tags", nil).Where(&Artifact{
		Author: pkg.Namespace,
		Name:   pkg.Name,
	}).Find(ctx)
	if err != nil {
		return nil, wrapErrorWithDetails(
			err,
			"get artifacts by FQN",
			fmt.Sprintf(
				"namespace=%s, name=%s",
				pkg.Namespace,
				pkg.Name,
			),
		)
	}

	return artifacts, nil
}

func (db *DB) CreateArtifactMeta(
	ctx context.Context,
	pkg *proto_gen.PackageName,
	versionHash string,
	tags ...string,
) error {
	if pkg == nil {
		return &BadInputError{
			Reason: "artifact with nil PackageName",
		}
	}

	if versionHash == "" || pkg.Namespace == "" ||
		pkg.Name == "" {
		return &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: namespace=%q, name=%q, hash=%q",
				pkg.Namespace,
				pkg.Name,
				versionHash,
			),
		}
	}

	detailString := fmt.Sprintf(
		"namespace=%q, name=%q, hash=%q, tags=%v",
		pkg.Namespace,
		pkg.Name,
		versionHash,
		tags,
	)

	err := db.dbGorm.Transaction(func(tx *gorm.DB) error {
		dbTx := db.UseTransaction(tx)
		err := gorm.G[Artifact](tx).Create(ctx, &Artifact{
			Author: pkg.Namespace,
			Name:   pkg.Name,
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
			err := dbTx.addTag(ctx, pkg, versionHash, tag)
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
	pkg *proto_gen.PackageName,
	versionHash string,
) error {
	if pkg == nil {
		return &BadInputError{
			Reason: "artifact with nil PackageName",
		}
	}

	if versionHash == "" || pkg.Namespace == "" ||
		pkg.Name == "" {
		return &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: namespace=%q, name=%q, hash=%q",
				pkg.Namespace,
				pkg.Name,
				versionHash,
			),
		}
	}

	return wrapErrorWithDetails(
		db.dbGorm.Delete(
			&Artifact{
				Author: pkg.Namespace,
				Name:   pkg.Name,
				Hash:   versionHash,
			},
		).Error,
		"delete artifact metadata",
		fmt.Sprintf(
			"namespace=%s, name=%s, hash=%s",
			pkg.Namespace,
			pkg.Name,
			versionHash,
		),
	)
}

func (db *DB) AddTag(
	ctx context.Context,
	pkg *proto_gen.PackageName,
	versionHash, tag string,
) error {
	if pkg == nil {
		return &BadInputError{
			Reason: "artifact with nil PackageName",
		}
	}

	if versionHash == "" || tag == "" || pkg.Namespace == "" ||
		pkg.Name == "" {
		return &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: namespace=%q, name=%q, hash=%q, tag=%q",
				pkg.Namespace,
				pkg.Name,
				versionHash,
				tag,
			),
		}
	}

	detailString := fmt.Sprintf(
		"namespace=%q, name=%q, hash=%q, tag=%q",
		pkg.Namespace,
		pkg.Name,
		versionHash,
		tag,
	)

	// Check that artifact exists
	count, err := gorm.G[Artifact](db.dbGorm).Where(Artifact{
		Author: pkg.Namespace,
		Name:   pkg.Name,
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
				"Artifact namespace=%q, name=%q, versionHash=%q does not exist",
				pkg.Namespace,
				pkg.Name,
				versionHash,
			),
		}
	}

	return db.addTag(ctx, pkg, versionHash, tag)
}

func (db *DB) addTag(
	ctx context.Context,
	pkg *proto_gen.PackageName,
	versionHash, tag string,
) error {
	tagObject := Tag{
		Author:  pkg.Namespace,
		Name:    pkg.Name,
		TagName: tag,
	}

	detailString := fmt.Sprintf(
		"namespace=%q, name=%q, hash=%q, tag=%q",
		pkg.Namespace,
		pkg.Name,
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
	pkg *proto_gen.PackageName,
	tag string,
) error {
	if pkg == nil {
		return &BadInputError{
			Reason: "artifact with nil PackageName",
		}
	}

	if tag == "" || pkg.Namespace == "" || pkg.Name == "" {
		return &BadInputError{
			Reason: fmt.Sprintf(
				"All parameters must be provided: namespace=%q, name=%q, tag=%q",
				pkg.Namespace,
				pkg.Name,
				tag,
			),
		}
	}

	return wrapErrorWithDetails(
		db.dbGorm.Delete(Tag{
			Author:  pkg.Namespace,
			Name:    pkg.Name,
			TagName: tag,
		}).Error,
		"delete tag",
		fmt.Sprintf(
			"namespace=%q, name=%q, tag=%q",
			pkg.Namespace,
			pkg.Name,
			tag,
		),
	)
}
