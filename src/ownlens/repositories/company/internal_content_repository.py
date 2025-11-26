"""
OwnLens - Company Domain: Internal Content Repository

Repository for company_internal_content table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.company.internal_content import InternalContent, InternalContentCreate, InternalContentUpdate

logger = logging.getLogger(__name__)


class InternalContentRepository(BaseRepository[InternalContent]):
    """Repository for company_internal_content table."""

    def __init__(self, connection_manager):
        """Initialize the internal content repository."""
        super().__init__(connection_manager, table_name="company_internal_content")
        self.internal_content_table = "company_internal_content"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "content_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "company_internal_content"

    # ============================================================================
    # INTERNAL CONTENT OPERATIONS
    # ============================================================================

    async def create_internal_content(self, content_data: InternalContentCreate) -> Optional[InternalContent]:
        """
        Create a new internal content record.
        
        Args:
            content_data: Internal content creation data (dict or Pydantic model)
            
        Returns:
            Created internal content with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(content_data, table_name=self.internal_content_table)
            if result:
                result = self._convert_json_to_lists(result)
                return InternalContent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating internal content: {e}")
            return None

    async def get_internal_content_by_id(self, content_id: UUID) -> Optional[InternalContent]:
        """Get internal content by ID."""
        try:
            result = await self.get_by_id(content_id)
            if result:
                result = self._convert_json_to_lists(result)
                return InternalContent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting internal content by ID {content_id}: {e}")
            return None

    async def get_internal_content_by_company(self, company_id: UUID, status: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[InternalContent]:
        """Get all internal content for a company."""
        try:
            filters = {"company_id": company_id}
            if status:
                filters["status"] = status
            
            results = await self.get_all(
                filters=filters,
                order_by="publish_time",
                limit=limit,
                offset=offset
            )
            return [InternalContent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting internal content by company {company_id}: {e}")
            return []

    async def get_internal_content_by_department(self, department_id: UUID, limit: int = 100, offset: int = 0) -> List[InternalContent]:
        """Get all internal content for a department."""
        try:
            from ..shared.query_builder import select_all
            
            query, params = (
                select_all()
                .table(self.internal_content_table)
                .where_contains("target_department_ids", department_id)
                .order_by("publish_time")
                .limit(limit)
                .offset(offset)
                .build()
            )
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [InternalContent(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting internal content by department {department_id}: {e}")
            return []

    async def get_published_internal_content(self, company_id: Optional[UUID] = None, limit: int = 100, offset: int = 0) -> List[InternalContent]:
        """Get all published internal content."""
        try:
            filters = {"status": "published"}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(
                filters=filters,
                order_by="publish_time",
                limit=limit,
                offset=offset
            )
            return [InternalContent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting published internal content: {e}")
            return []

    async def get_internal_content_by_author(self, author_employee_id: UUID, limit: int = 100, offset: int = 0) -> List[InternalContent]:
        """Get all internal content by an author."""
        try:
            results = await self.get_all(
                filters={"author_employee_id": author_employee_id},
                order_by="publish_time",
                limit=limit,
                offset=offset
            )
            return [InternalContent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting internal content by author {author_employee_id}: {e}")
            return []

    async def update_internal_content(self, content_id: UUID, content_data: InternalContentUpdate) -> Optional[InternalContent]:
        """Update internal content data."""
        try:
            data = content_data.model_dump(exclude_unset=True)
            
            result = await self.update(content_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return InternalContent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating internal content {content_id}: {e}")
            return None

    async def delete_internal_content(self, content_id: UUID) -> bool:
        """Delete internal content and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(content_id)
            
        except Exception as e:
            logger.error(f"Error deleting internal content {content_id}: {e}")
            return False

