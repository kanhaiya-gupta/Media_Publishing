"""
OwnLens Shared Services
========================

Shared service components for OwnLens:
- BaseService: Abstract base class for all services
- ServiceError: Base exception for service errors
- ValidationError: Exception for validation errors
- NotFoundError: Exception for not found errors
- ConflictError: Exception for conflict errors
- ValidationService: Data validation and business rule validation
- NotificationService: Multi-channel notification capabilities
- AuditService: Audit logging and compliance tracking
- CacheService: Caching and performance optimization
"""

from .base_service import (
    BaseService,
    ServiceError,
    ValidationError,
    NotFoundError,
    ConflictError,
)

from .validation_service import ValidationService
from .notification_service import NotificationService
from .audit_service import AuditService
from .cache_service import CacheService

__all__ = [
    "BaseService",
    "ServiceError",
    "ValidationError",
    "NotFoundError",
    "ConflictError",
    "ValidationService",
    "NotificationService",
    "AuditService",
    "CacheService",
]
