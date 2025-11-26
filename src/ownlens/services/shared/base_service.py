"""
Base Service - OwnLens
======================

Abstract base service class for OwnLens.
Provides common functionality and patterns for all service implementations.

This class provides:
- Repository dependency injection
- Error handling and logging infrastructure
- Input validation framework
- Transaction management support
- Common service patterns and utilities
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, TypeVar, Generic
from datetime import datetime
import logging
import traceback
import asyncio
from contextlib import asynccontextmanager

from pydantic import BaseModel

from ...repositories.shared.base_repository import BaseRepository

# Type variables for generic service typing
T = TypeVar('T', bound=BaseModel)
CreateT = TypeVar('CreateT', bound=BaseModel)
UpdateT = TypeVar('UpdateT', bound=BaseModel)
InDBT = TypeVar('InDBT', bound=BaseModel)

logger = logging.getLogger(__name__)


class ServiceError(Exception):
    """Base exception for service errors."""
    
    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        super().__init__(self.message)


class ValidationError(ServiceError):
    """Exception raised for validation errors."""
    pass


class NotFoundError(ServiceError):
    """Exception raised when a resource is not found."""
    pass


class ConflictError(ServiceError):
    """Exception raised when there's a conflict with existing data."""
    pass


class BaseService(ABC, Generic[T, CreateT, UpdateT, InDBT]):
    """
    Abstract base service class for OwnLens.
    
    Provides common functionality and patterns for all service implementations,
    including repository dependency injection, error handling, validation, and logging.
    """
    
    def __init__(self, repository: BaseRepository, service_name: str = None):
        """
        Initialize the base service.
        
        Args:
            repository: Repository instance for data access
            service_name: Optional service name for logging
        """
        self.repository = repository
        self.service_name = service_name or self.__class__.__name__
        self.logger = logging.getLogger(f"{__name__}.{self.service_name}")
        
        # Service configuration
        self._config = {
            'enable_caching': True,
            'enable_audit_logging': True,
            'enable_validation': True,
            'max_retry_attempts': 3,
            'retry_delay': 1.0
        }
    
    # ==================== ABSTRACT METHODS ====================
    
    @abstractmethod
    def get_model_class(self) -> Type[T]:
        """Get the Pydantic Base model class for this service."""
        pass
    
    @abstractmethod
    def get_create_model_class(self) -> Type[CreateT]:
        """Get the Create model class for this service."""
        pass
    
    @abstractmethod
    def get_update_model_class(self) -> Type[UpdateT]:
        """Get the Update model class for this service."""
        pass
    
    @abstractmethod
    def get_in_db_model_class(self) -> Type[InDBT]:
        """Get the InDB model class for this service."""
        pass
    
    # ==================== CONFIGURATION METHODS ====================
    
    def configure(self, **kwargs) -> None:
        """
        Configure service settings.
        
        Args:
            **kwargs: Configuration options
        """
        self._config.update(kwargs)
        self.logger.info(f"Service {self.service_name} configured with: {kwargs}")
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self._config.get(key, default)
    
    # ==================== VALIDATION METHODS ====================
    
    async def validate_input(self, data: Any, validation_type: str = "general") -> bool:
        """
        Validate service input data.
        
        Args:
            data: Data to validate
            validation_type: Type of validation to perform
            
        Returns:
            True if validation passes
            
        Raises:
            ValidationError: If validation fails
        """
        if not self.get_config('enable_validation', True):
            return True
        
        try:
            if validation_type == "create":
                model_class = self.get_create_model_class()
            elif validation_type == "update":
                model_class = self.get_update_model_class()
            else:
                model_class = self.get_model_class()
            
            # Validate using Pydantic model
            if hasattr(data, 'model_dump'):
                # Already a Pydantic model
                validated_data = data
            else:
                # Convert dict to Pydantic model
                validated_data = model_class(**data)
            
            self.logger.debug(f"Input validation passed for {validation_type}")
            return True
            
        except Exception as e:
            error_msg = f"Validation failed for {validation_type}: {str(e)}"
            self.logger.warning(error_msg)
            raise ValidationError(error_msg, "VALIDATION_ERROR", {"validation_type": validation_type, "error": str(e)})
    
    async def validate_business_rules(self, data: Any, operation: str = "create") -> bool:
        """
        Validate business rules for the operation.
        
        Args:
            data: Data to validate
            operation: Operation being performed (create, update, delete)
            
        Returns:
            True if business rules pass
            
        Raises:
            ValidationError: If business rules fail
        """
        # Override in subclasses for specific business rule validation
        return True
    
    # ==================== ERROR HANDLING METHODS ====================
    
    async def handle_error(self, error: Exception, context: str, operation: str = None) -> None:
        """
        Handle service errors with logging and error transformation.
        
        Args:
            error: Exception that occurred
            context: Context where error occurred
            operation: Operation being performed
        """
        error_context = {
            "service": self.service_name,
            "context": context,
            "operation": operation,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if isinstance(error, ServiceError):
            # Already a service error, just log it
            self.logger.error(f"Service error in {context}: {error.message}", extra=error_context)
        else:
            # Convert to service error and log
            service_error = ServiceError(
                message=f"Unexpected error in {context}: {str(error)}",
                error_code="INTERNAL_ERROR",
                details=error_context
            )
            self.logger.error(f"Unexpected error in {context}: {str(error)}", extra=error_context, exc_info=True)
            raise service_error
    
    def create_error(self, message: str, error_code: str = None, details: Dict[str, Any] = None) -> ServiceError:
        """
        Create a service error with proper context.
        
        Args:
            message: Error message
            error_code: Error code
            details: Additional error details
            
        Returns:
            ServiceError instance
        """
        return ServiceError(
            message=message,
            error_code=error_code,
            details=details or {"service": self.service_name}
        )
    
    # ==================== TRANSACTION METHODS ====================
    
    @asynccontextmanager
    async def transaction(self):
        """
        Context manager for database transactions.
        
        Yields:
            Transaction context
        """
        try:
            # Start transaction
            await self.repository.connection_manager.begin_transaction()
            self.logger.debug("Transaction started")
            
            yield self.repository.connection_manager
            
            # Commit transaction
            await self.repository.connection_manager.commit_transaction()
            self.logger.debug("Transaction committed")
            
        except Exception as e:
            # Rollback transaction
            await self.repository.connection_manager.rollback_transaction()
            self.logger.error(f"Transaction rolled back due to error: {str(e)}")
            raise
    
    # ==================== RETRY METHODS ====================
    
    async def retry_operation(self, operation, *args, max_attempts: int = None, delay: float = None, **kwargs):
        """
        Retry an operation with exponential backoff.
        
        Args:
            operation: Operation to retry
            *args: Positional arguments for operation
            max_attempts: Maximum number of retry attempts
            delay: Initial delay between retries
            **kwargs: Keyword arguments for operation
            
        Returns:
            Operation result
            
        Raises:
            ServiceError: If all retry attempts fail
        """
        max_attempts = max_attempts or self.get_config('max_retry_attempts', 3)
        delay = delay or self.get_config('retry_delay', 1.0)
        
        last_error = None
        
        for attempt in range(max_attempts):
            try:
                if asyncio.iscoroutinefunction(operation):
                    return await operation(*args, **kwargs)
                else:
                    return operation(*args, **kwargs)
                    
            except Exception as e:
                last_error = e
                if attempt < max_attempts - 1:
                    wait_time = delay * (2 ** attempt)  # Exponential backoff
                    self.logger.warning(f"Operation failed (attempt {attempt + 1}/{max_attempts}), retrying in {wait_time}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"Operation failed after {max_attempts} attempts: {str(e)}")
                    break
        
        # If we get here, all attempts failed
        raise self.create_error(
            message=f"Operation failed after {max_attempts} attempts: {str(last_error)}",
            error_code="RETRY_EXHAUSTED",
            details={"max_attempts": max_attempts, "last_error": str(last_error)}
        )
    
    # ==================== LOGGING METHODS ====================
    
    def log_operation(self, operation: str, details: Dict[str, Any] = None, level: str = "info") -> None:
        """
        Log a service operation.
        
        Args:
            operation: Operation being performed
            details: Additional operation details
            level: Log level (info, debug, warning, error)
        """
        log_data = {
            "service": self.service_name,
            "operation": operation,
            "timestamp": datetime.utcnow().isoformat(),
            "details": details or {}
        }
        
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(f"Service operation: {operation}", extra=log_data)
    
    def log_performance(self, operation: str, duration: float, details: Dict[str, Any] = None) -> None:
        """
        Log performance metrics for an operation.
        
        Args:
            operation: Operation performed
            duration: Operation duration in seconds
            details: Additional performance details
        """
        perf_data = {
            "service": self.service_name,
            "operation": operation,
            "duration_seconds": duration,
            "timestamp": datetime.utcnow().isoformat(),
            "details": details or {}
        }
        
        self.logger.info(f"Performance metric: {operation} took {duration:.3f}s", extra=perf_data)
    
    # ==================== UTILITY METHODS ====================
    
    def get_service_info(self) -> Dict[str, Any]:
        """
        Get service information.
        
        Returns:
            Dictionary containing service information
        """
        return {
            "service_name": self.service_name,
            "repository_type": type(self.repository).__name__,
            "model_class": self.get_model_class().__name__,
            "config": self._config,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on the service.
        
        Returns:
            Health check results
        """
        try:
            # Check repository connection
            await self.repository.connection_manager.execute_query("SELECT 1", [])
            
            return {
                "status": "healthy",
                "service": self.service_name,
                "repository": "connected",
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "service": self.service_name,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    # ==================== COMMON CRUD OPERATIONS ====================
    
    async def create(self, data: CreateT) -> Optional[InDBT]:
        """
        Create a new entity.
        
        Args:
            data: Entity creation data
            
        Returns:
            Created entity or None if creation failed
        """
        start_time = datetime.utcnow()
        
        try:
            # Validate input
            await self.validate_input(data, "create")
            await self.validate_business_rules(data, "create")
            
            # Log operation
            self.log_operation("create", {"entity_type": self.get_model_class().__name__})
            
            # Create entity
            if hasattr(data, 'model_dump'):
                # Pydantic model
                data_dict = data.model_dump(exclude_none=True)
            else:
                # Plain dictionary
                data_dict = data
            
            # Use repository's create method (which handles table_name internally)
            if hasattr(self.repository, 'create'):
                result = await self.repository.create(data_dict)
            else:
                raise ServiceError("Repository does not have a create method")
            
            if result:
                duration = (datetime.utcnow() - start_time).total_seconds()
                self.log_performance("create", duration)
                return self.get_in_db_model_class()(**result)
            
            return None
            
        except Exception as e:
            await self.handle_error(e, "create", "create")
            raise
    
    async def get_by_id(self, entity_id: Any) -> Optional[InDBT]:
        """
        Get an entity by ID.
        
        Args:
            entity_id: Entity ID (can be int, str, or UUID)
            
        Returns:
            Entity or None if not found
        """
        start_time = datetime.utcnow()
        
        try:
            # Log operation
            self.log_operation("get_by_id", {"entity_id": str(entity_id)})
            
            # Get entity
            result = await self.repository.get_by_id(entity_id)
            
            if result:
                duration = (datetime.utcnow() - start_time).total_seconds()
                self.log_performance("get_by_id", duration)
                return self.get_in_db_model_class()(**result)
            
            return None
            
        except Exception as e:
            await self.handle_error(e, "get_by_id", "read")
            raise
    
    async def update(self, entity_id: Any, data: UpdateT) -> Optional[InDBT]:
        """
        Update an entity.
        
        Args:
            entity_id: Entity ID (can be int, str, or UUID)
            data: Updated entity data
            
        Returns:
            Updated entity or None if update failed
        """
        start_time = datetime.utcnow()
        
        try:
            # Validate input
            await self.validate_input(data, "update")
            await self.validate_business_rules(data, "update")
            
            # Log operation
            self.log_operation("update", {"entity_id": str(entity_id)})
            
            # Update entity
            if hasattr(data, 'model_dump'):
                # Pydantic model
                data_dict = data.model_dump(exclude_unset=True)
            else:
                # Plain dictionary
                data_dict = data
            
            # Use repository's update method
            if hasattr(self.repository, 'update'):
                result = await self.repository.update(entity_id, data_dict)
            else:
                raise ServiceError("Repository does not have an update method")
            
            if result:
                duration = (datetime.utcnow() - start_time).total_seconds()
                self.log_performance("update", duration)
                return self.get_in_db_model_class()(**result)
            
            return None
            
        except Exception as e:
            await self.handle_error(e, "update", "update")
            raise
    
    async def delete(self, entity_id: Any) -> bool:
        """
        Delete an entity.
        
        Args:
            entity_id: Entity ID (can be int, str, or UUID)
            
        Returns:
            True if deletion successful
        """
        start_time = datetime.utcnow()
        
        try:
            # Log operation
            self.log_operation("delete", {"entity_id": str(entity_id)})
            
            # Delete entity
            if hasattr(self.repository, 'delete'):
                result = await self.repository.delete(entity_id)
            else:
                raise ServiceError("Repository does not have a delete method")
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.log_performance("delete", duration)
            
            return result
            
        except Exception as e:
            await self.handle_error(e, "delete", "delete")
            raise

