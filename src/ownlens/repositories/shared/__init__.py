"""
OwnLens Shared Repository Infrastructure
=========================================

Database-agnostic shared repository infrastructure for OwnLens.
"""

from .base_repository import BaseRepository
from .query_builder import (
    QueryBuilder, QueryType, OrderDirection, PaginationParams,
    select, select_all, count, insert_into, update_table, delete_from
)
from .repository_factory import (
    RepositoryFactory, get_repository_factory, set_repository_factory,
    reset_repository_factory, register_repository, RepositoryMixin,
    RepositoryContext, repository_context
)
from .connection_manager import AsyncPGConnectionManager, create_connection_manager

__all__ = [
    # Base Repository
    'BaseRepository',
    
    # Query Builder
    'QueryBuilder',
    'QueryType',
    'OrderDirection',
    'PaginationParams',
    'select',
    'select_all',
    'count',
    'insert_into',
    'update_table',
    'delete_from',
    
    # Repository Factory
    'RepositoryFactory',
    'get_repository_factory',
    'set_repository_factory',
    'reset_repository_factory',
    'register_repository',
    'RepositoryMixin',
    'RepositoryContext',
    'repository_context',
    
    # Connection Manager
    'AsyncPGConnectionManager',
    'create_connection_manager'
]

