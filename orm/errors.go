package orm

import (
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// DatabaseError wraps database-related errors from GORM
type DatabaseError struct {
	Inner error
}

func (e *DatabaseError) Error() string {
	return "Database operation failed: " + e.Inner.Error()
}

func (e *DatabaseError) Unwrap() error {
	return e.Inner
}

// NotFoundError represents when an artifact or record is not found
type NotFoundError struct {
	Search string
}

func (e *NotFoundError) Error() string {
	return "Record not found for search: " + e.Search
}

// ConflictError represents when there's a conflict (e.g., duplicate records)
type ConflictError struct {
	Conflict string
}

func (e *ConflictError) Error() string {
	return "Conflict error for: " + e.Conflict
}

// GenericError wraps unexpected errors
type GenericError struct {
	Inner error
}

func (e *GenericError) Error() string {
	return "An unexpected error occurred: " + e.Inner.Error()
}

func (e *GenericError) Unwrap() error {
	return e.Inner
}

type BadInputError struct {
	Reason string
}

func (e *BadInputError) Error() string {
	return "Bad input: " + e.Reason
}

// wrapErrorWithDetails creates a more specific error message
func wrapErrorWithDetails(err error, operation, details string) error {
	if err == nil {
		return nil
	}

	// Handle specific GORM errors with details
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return &NotFoundError{Search: fmt.Sprintf("%s (%s)", operation, details)}
	}

	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return &ConflictError{Conflict: fmt.Sprintf("%s (%s)", operation, details)}
	}

	// For other database errors, wrap with DatabaseError
	return &DatabaseError{Inner: fmt.Errorf("%s: %w", operation, err)}
}
