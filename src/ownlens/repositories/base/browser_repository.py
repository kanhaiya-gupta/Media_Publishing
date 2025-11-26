"""
OwnLens - Base Domain: Browser Repository

Repository for browsers table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.base.browser import Browser, BrowserCreate, BrowserUpdate

logger = logging.getLogger(__name__)


class BrowserRepository(BaseRepository[Browser]):
    """Repository for browsers table."""

    def __init__(self, connection_manager):
        """Initialize the browser repository."""
        super().__init__(connection_manager, table_name="browsers")
        self.browsers_table = "browsers"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "browser_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "browsers"

    # ============================================================================
    # BROWSER OPERATIONS
    # ============================================================================

    async def create_browser(self, browser_data: BrowserCreate) -> Optional[Browser]:
        """
        Create a new browser record.
        
        Args:
            browser_data: Browser creation data (dict or BrowserCreate model)
            
        Returns:
            Created browser with generated ID, or None if creation failed
        """
        try:
            # Convert dict to BrowserCreate model if needed (this runs validators)
            if isinstance(browser_data, dict):
                browser_data = BrowserCreate.model_validate(browser_data)
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(browser_data, table_name=self.browsers_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Browser(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating browser: {e}")
            return None

    async def get_browser_by_id(self, browser_id: UUID) -> Optional[Browser]:
        """Get browser by ID."""
        try:
            result = await self.get_by_id(browser_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Browser(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting browser by ID {browser_id}: {e}")
            return None

    async def get_browser_by_code(self, browser_code: str) -> Optional[Browser]:
        """Get browser by code."""
        try:
            result = await self.find_by_field("browser_code", browser_code)
            if result:
                result = self._convert_json_to_lists(result)
                return Browser(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting browser by code {browser_code}: {e}")
            return None

    async def get_active_browsers(self) -> List[Browser]:
        """Get all active browsers."""
        try:
            results = await self.get_all(filters={"is_active": True}, order_by="browser_name")
            return [Browser(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active browsers: {e}")
            return []

    async def get_browsers_by_vendor(self, vendor: str) -> List[Browser]:
        """Get browsers by vendor."""
        try:
            results = await self.find_all_by_field("vendor", vendor)
            return [Browser(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting browsers by vendor {vendor}: {e}")
            return []

    async def update_browser(self, browser_id: UUID, browser_data: BrowserUpdate) -> Optional[Browser]:
        """Update browser data."""
        try:
            data = browser_data.model_dump(exclude_unset=True)
            
            result = await self.update(browser_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Browser(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating browser {browser_id}: {e}")
            return None

    async def delete_browser(self, browser_id: UUID) -> bool:
        """Delete browser and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(browser_id)
            
        except Exception as e:
            logger.error(f"Error deleting browser {browser_id}: {e}")
            return False

