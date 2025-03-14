"""
Exceptions for the Alpha Trade Exchange database operations.

This module defines a hierarchy of exceptions that can occur during database
operations, providing specific error types for different failure scenarios.
"""

class DatabaseError(Exception):
    """Base exception for all database-related errors."""
    pass

class ValidationError(DatabaseError):
    """Raised when input validation fails."""
    pass

class IntegrityError(DatabaseError):
    """Raised when database integrity constraints are violated."""
    pass

class StateError(DatabaseError):
    """Raised when an invalid state transition is attempted."""
    pass

class ConnectionError(DatabaseError):
    """Raised when database connection issues occur."""
    pass

# Validation-specific errors
class InvalidAmountError(ValidationError):
    """Raised when an amount (TAO/alpha) is invalid."""
    pass

class InvalidTimestampError(ValidationError):
    """Raised when a timestamp is invalid or out of order."""
    pass

# State-specific errors
class PositionNotFoundError(StateError):
    """Raised when attempting to operate on a non-existent position."""
    pass

class InsufficientAlphaError(StateError):
    """Raised when attempting to close more alpha than available."""
    pass

class InvalidStateTransitionError(StateError):
    """Raised when attempting an invalid position state transition."""
    pass

# Integrity-specific errors
class DuplicateEventError(IntegrityError):
    """Raised when attempting to process a duplicate event."""
    pass

class OrphanedRecordError(IntegrityError):
    """Raised when detecting orphaned position-trade mappings."""
    pass 