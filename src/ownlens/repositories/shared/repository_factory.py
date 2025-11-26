"""
OwnLens - Repository Factory
==============================

Factory for creating and managing repository instances.
"""

from typing import Dict, Type, Any, Optional
import logging
from abc import ABC, abstractmethod

from .base_repository import BaseRepository

logger = logging.getLogger(__name__)


class RepositoryFactory:
    """
    Factory for creating and managing repository instances.
    
    Provides centralized repository creation and dependency injection.
    """
    
    def __init__(self, connection_manager):
        """
        Initialize the repository factory.
        
        Args:
            connection_manager: Database connection manager (required)
        """
        if connection_manager is None:
            raise ValueError("connection_manager is required for RepositoryFactory")
        self.connection_manager = connection_manager
        self._repositories: Dict[str, BaseRepository] = {}
        self._repository_classes: Dict[str, Type[BaseRepository]] = {}
        self.logger = logging.getLogger(f"{__name__}.RepositoryFactory")
    
    def register_repository(self, name: str, repository_class: Type[BaseRepository]) -> None:
        """
        Register a repository class.
        
        Args:
            name: Repository name
            repository_class: Repository class
        """
        if not issubclass(repository_class, BaseRepository):
            raise ValueError(f"Repository class must inherit from BaseRepository")
        
        self._repository_classes[name] = repository_class
        self.logger.info(f"Registered repository: {name}")
    
    def get_repository(self, name: str, **kwargs) -> BaseRepository:
        """
        Get a repository instance.
        
        Args:
            name: Repository name
            **kwargs: Additional arguments for repository initialization
            
        Returns:
            Repository instance
        """
        if name not in self._repository_classes:
            raise ValueError(f"Repository '{name}' not registered")
        
        # Check if we already have an instance
        if name in self._repositories:
            return self._repositories[name]
        
        # Create new instance
        repository_class = self._repository_classes[name]
        repository = repository_class(self.connection_manager, **kwargs)
        
        # Store instance for reuse
        self._repositories[name] = repository
        
        self.logger.info(f"Created repository instance: {name}")
        return repository
    
    def create_repository(self, repository_class: Type[BaseRepository], **kwargs) -> BaseRepository:
        """
        Create a new repository instance without caching.
        
        Args:
            repository_class: Repository class
            **kwargs: Additional arguments for repository initialization
            
        Returns:
            New repository instance
        """
        if not issubclass(repository_class, BaseRepository):
            raise ValueError(f"Repository class must inherit from BaseRepository")
        
        repository = repository_class(self.connection_manager, **kwargs)
        self.logger.info(f"Created new repository instance: {repository_class.__name__}")
        return repository
    
    def clear_cache(self) -> None:
        """Clear the repository cache."""
        self._repositories.clear()
        self.logger.info("Repository cache cleared")
    
    def get_registered_repositories(self) -> Dict[str, Type[BaseRepository]]:
        """Get all registered repository classes."""
        return self._repository_classes.copy()
    
    def get_cached_repositories(self) -> Dict[str, BaseRepository]:
        """Get all cached repository instances."""
        return self._repositories.copy()
    
    def is_registered(self, name: str) -> bool:
        """Check if a repository is registered."""
        return name in self._repository_classes
    
    def is_cached(self, name: str) -> bool:
        """Check if a repository instance is cached."""
        return name in self._repositories
    
    def remove_repository(self, name: str) -> None:
        """Remove a repository from cache and registration."""
        if name in self._repositories:
            del self._repositories[name]
        
        if name in self._repository_classes:
            del self._repository_classes[name]
        
        self.logger.info(f"Removed repository: {name}")
    
    async def initialize_all(self) -> Dict[str, bool]:
        """
        Initialize all cached repositories.
        
        Returns:
            Dictionary mapping repository names to initialization success status
        """
        results = {}
        
        for name, repository in self._repositories.items():
            try:
                success = await repository.initialize()
                results[name] = success
                if success:
                    self.logger.info(f"Repository {name} initialized successfully")
                else:
                    self.logger.error(f"Repository {name} initialization failed")
            except Exception as e:
                self.logger.error(f"Error initializing repository {name}: {str(e)}")
                results[name] = False
        
        return results


# ============================================================================
# Global Repository Factory Instance
# ============================================================================

# Global factory instance
_factory: Optional[RepositoryFactory] = None


def get_repository_factory(connection_manager=None) -> Optional[RepositoryFactory]:
    """
    Get the global repository factory instance.
    
    Args:
        connection_manager: Database connection manager (required if factory not initialized)
    
    Returns:
        RepositoryFactory instance or None if not initialized and no connection_manager provided
    """
    global _factory
    if _factory is None:
        if connection_manager is None:
            logger.warning("RepositoryFactory not initialized. Call set_repository_factory() or provide connection_manager.")
            return None
        _factory = RepositoryFactory(connection_manager)
    return _factory


def set_repository_factory(factory: RepositoryFactory) -> None:
    """Set the global repository factory instance."""
    global _factory
    _factory = factory


def reset_repository_factory() -> None:
    """Reset the global repository factory instance."""
    global _factory
    _factory = None


# ============================================================================
# Repository Registration Decorator
# ============================================================================

def register_repository(name: str):
    """
    Decorator to register a repository class.
    
    Args:
        name: Repository name
    """
    def decorator(repository_class: Type[BaseRepository]) -> Type[BaseRepository]:
        factory = get_repository_factory()
        factory.register_repository(name, repository_class)
        return repository_class
    
    return decorator


# ============================================================================
# Repository Mixins
# ============================================================================

class RepositoryMixin:
    """Mixin class for adding repository functionality to other classes."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._repository_factory = get_repository_factory()
    
    def get_repository(self, name: str, **kwargs) -> BaseRepository:
        """Get a repository instance."""
        return self._repository_factory.get_repository(name, **kwargs)
    
    def create_repository(self, repository_class: Type[BaseRepository], **kwargs) -> BaseRepository:
        """Create a new repository instance."""
        return self._repository_factory.create_repository(repository_class, **kwargs)


# ============================================================================
# Repository Context Manager
# ============================================================================

class RepositoryContext:
    """Context manager for repository operations."""
    
    def __init__(self, repository: BaseRepository):
        self.repository = repository
        self.logger = logging.getLogger(f"{__name__}.RepositoryContext")
    
    async def __aenter__(self) -> BaseRepository:
        """Enter the context."""
        await self.repository.initialize()
        return self.repository
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context."""
        # Cleanup if needed
        pass


def repository_context(repository: BaseRepository) -> RepositoryContext:
    """Create a repository context manager."""
    return RepositoryContext(repository)

