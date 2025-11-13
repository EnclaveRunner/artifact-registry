package registry

import (
	"artifact-registry/orm"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Static errors to avoid err113 violations
var ErrRegistryNil = errors.New("registry is nil")

// ServiceError represents public-facing errors from the registry service
type ServiceError struct {
	Code    codes.Code
	Message string
	Inner   error
}

func (e *ServiceError) Error() string {
	return e.Message
}

func (e *ServiceError) Unwrap() error {
	return e.Inner
}

func (e *ServiceError) GRPCStatus() *status.Status {
	return status.New(e.Code, e.Message)
}

// wrapServiceError converts internal errors to user-friendly service errors
func wrapServiceError(err error, operation string) error {
	if err == nil {
		return nil
	}

	// Handle ORM-specific errors
	var notFoundErr *orm.NotFoundError
	if errors.As(err, &notFoundErr) {
		return &ServiceError{
			Code:    codes.NotFound,
			Message: "Artifact not found for " + operation,
			Inner:   err,
		}
	}

	var conflictErr *orm.ConflictError
	if errors.As(err, &conflictErr) {
		return &ServiceError{
			Code:    codes.AlreadyExists,
			Message: "Artifact already exists for " + operation,
			Inner:   err,
		}
	}

	var dbErr *orm.DatabaseError
	if errors.As(err, &dbErr) {
		return &ServiceError{
			Code:    codes.Internal,
			Message: "Internal server error during " + operation,
			Inner:   err,
		}
	}

	// Handle generic errors
	return &ServiceError{
		Code:    codes.Internal,
		Message: "Internal server error during " + operation,
		Inner:   err,
	}
}

// Common error constructors for specific operations
func newInvalidIdentifierError() error {
	return &ServiceError{
		Code:    codes.InvalidArgument,
		Message: "Invalid artifact identifier provided",
		Inner:   ErrInvalidIdentifier,
	}
}

func newRegistryUnavailableError(operation string) error {
	return &ServiceError{
		Code:    codes.Unavailable,
		Message: "Registry service unavailable for " + operation,
		Inner:   ErrRegistryNil,
	}
}
