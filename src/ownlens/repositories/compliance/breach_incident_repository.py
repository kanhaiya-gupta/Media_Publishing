"""
OwnLens - Compliance Domain: Breach Incident Repository

Repository for compliance_breach_incidents table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.compliance.breach_incident import BreachIncident, BreachIncidentCreate, BreachIncidentUpdate

logger = logging.getLogger(__name__)


class BreachIncidentRepository(BaseRepository[BreachIncident]):
    """Repository for compliance_breach_incidents table."""

    def __init__(self, connection_manager):
        """Initialize the breach incident repository."""
        super().__init__(connection_manager, table_name="compliance_breach_incidents")
        self.breach_incidents_table = "compliance_breach_incidents"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "incident_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "compliance_breach_incidents"

    # ============================================================================
    # BREACH INCIDENT OPERATIONS
    # ============================================================================

    async def create_breach_incident(self, incident_data: Union[BreachIncidentCreate, Dict[str, Any]]) -> Optional[BreachIncident]:
        """
        Create a new breach incident record.
        
        Args:
            incident_data: Breach incident creation data (Pydantic model or dict)
            
        Returns:
            Created breach incident with generated ID, or None if creation failed
        """
        try:
            # Handle both Pydantic model and dict
            if isinstance(incident_data, dict):
                incident_data = BreachIncidentCreate.model_validate(incident_data)
            
            data = incident_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.breach_incidents_table)
            if result:
                result = self._convert_json_to_lists(result)
                return BreachIncident(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating breach incident: {e}")
            return None

    async def get_breach_incident_by_id(self, incident_id: UUID) -> Optional[BreachIncident]:
        """Get breach incident by ID."""
        try:
            result = await self.get_by_id(incident_id)
            if result:
                result = self._convert_json_to_lists(result)
                return BreachIncident(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting breach incident by ID {incident_id}: {e}")
            return None

    async def get_incidents_by_status(self, status: str, limit: int = 100) -> List[BreachIncident]:
        """Get incidents by status."""
        try:
            results = await self.get_all(
                filters={"status": status},
                order_by="incident_date",
                limit=limit
            )
            return [BreachIncident(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting incidents by status {status}: {e}")
            return []

    async def get_incidents_by_severity(self, severity: str, limit: int = 100) -> List[BreachIncident]:
        """Get incidents by severity."""
        try:
            results = await self.get_all(
                filters={"severity": severity},
                order_by="incident_date",
                limit=limit
            )
            return [BreachIncident(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting incidents by severity {severity}: {e}")
            return []

    async def get_active_incidents(self, limit: int = 100) -> List[BreachIncident]:
        """Get active incidents."""
        try:
            results = await self.get_all(
                filters={"status": "active"},
                order_by="incident_date",
                limit=limit
            )
            return [BreachIncident(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active incidents: {e}")
            return []

    async def update_breach_incident(self, incident_id: UUID, incident_data: BreachIncidentUpdate) -> Optional[BreachIncident]:
        """Update breach incident data."""
        try:
            data = incident_data.model_dump(exclude_unset=True)
            
            result = await self.update(incident_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return BreachIncident(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating breach incident {incident_id}: {e}")
            return None

    async def delete_breach_incident(self, incident_id: UUID) -> bool:
        """Delete breach incident and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(incident_id)
            
        except Exception as e:
            logger.error(f"Error deleting breach incident {incident_id}: {e}")
            return False

