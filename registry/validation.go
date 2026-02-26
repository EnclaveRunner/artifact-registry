package registry

import (
	"artifact-registry/proto_gen"
	"errors"

	"google.golang.org/grpc/codes"
)

var (
	// Static errors to avoid err113 violations
	ErrInvalidIdentifier = errors.New("no valid identifier provided")
	ErrEmptyTag          = errors.New("tag cannot be empty")
	ErrEmptyVersionHash  = errors.New("versionHash cannot be empty")
)

func validateFQN(pkg *proto_gen.PackageName) error {
	if pkg == nil || pkg.Namespace == "" || pkg.Name == "" {
		return &ServiceError{
			Code:    codes.InvalidArgument,
			Message: "PackageName must have namespace and name",
			Inner:   ErrInvalidIdentifier,
		}
	}

	return nil
}

func validateArtifactIdentifier(id *proto_gen.ArtifactIdentifier) error {
	if err := validateFQN(id.Package); err != nil {
		return err
	}

	switch identifier := id.Identifier.(type) {
	case *proto_gen.ArtifactIdentifier_VersionHash:
		if identifier.VersionHash == "" {
			return &ServiceError{
				Code:    codes.InvalidArgument,
				Message: "versionHash cannot be empty",
				Inner:   ErrEmptyVersionHash,
			}
		}
	case *proto_gen.ArtifactIdentifier_Tag:
		if identifier.Tag == "" {
			return &ServiceError{
				Code:    codes.InvalidArgument,
				Message: "tag cannot be empty",
				Inner:   ErrEmptyTag,
			}
		}
	default:
		return newInvalidIdentifierError()
	}

	return nil
}

func validateAddRemoveTagRequest(req *proto_gen.AddRemoveTagRequest) error {
	if err := validateFQN(req.Package); err != nil {
		return err
	}

	if req.Tag == "" {
		return &ServiceError{
			Code:    codes.InvalidArgument,
			Message: "tag cannot be empty",
			Inner:   ErrEmptyTag,
		}
	}

	if req.VersionHash == "" {
		return &ServiceError{
			Code:    codes.InvalidArgument,
			Message: "versionHash cannot be empty",
			Inner:   ErrEmptyVersionHash,
		}
	}

	return nil
}
