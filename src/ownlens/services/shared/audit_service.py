"""
Audit Service - OwnLens
========================

Audit service for OwnLens.
Provides comprehensive audit logging and compliance tracking.

This service provides:
- Audit logging and compliance tracking
- Security event logging and monitoring
- User action tracking and audit trails
- Compliance report generation
- Audit data retention and archival
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum
import logging
import uuid
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class AuditLevel(Enum):
    """Audit log levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AuditCategory(Enum):
    """Audit log categories."""
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    DATA_ACCESS = "data_access"
    DATA_MODIFICATION = "data_modification"
    SYSTEM_EVENT = "system_event"
    SECURITY_EVENT = "security_event"
    COMPLIANCE_EVENT = "compliance_event"
    USER_ACTION = "user_action"
    ADMIN_ACTION = "admin_action"
    API_ACCESS = "api_access"
    # OwnLens domain-specific events
    CUSTOMER_EVENT = "customer_event"
    EDITORIAL_EVENT = "editorial_event"
    COMPANY_EVENT = "company_event"
    CONTENT_EVENT = "content_event"
    MEDIA_EVENT = "media_event"
    ML_MODEL_EVENT = "ml_model_event"
    DATA_QUALITY_EVENT = "data_quality_event"


class AuditStatus(Enum):
    """Audit event status."""
    SUCCESS = "success"
    FAILURE = "failure"
    PENDING = "pending"
    CANCELLED = "cancelled"


@dataclass
class AuditEvent:
    """Audit event record."""
    id: str
    timestamp: datetime
    level: AuditLevel
    category: AuditCategory
    status: AuditStatus
    user_id: Optional[str]
    session_id: Optional[str]
    action: str
    resource_type: str
    resource_id: Optional[str]
    description: str
    details: Dict[str, Any]
    ip_address: Optional[str]
    user_agent: Optional[str]
    source: str
    tags: List[str]
    created_at: datetime


class AuditService:
    """
    Audit service for OwnLens.
    
    Provides comprehensive audit logging and compliance tracking
    for all OwnLens operations and events.
    """
    
    def __init__(self):
        """Initialize the audit service."""
        self.logger = logging.getLogger(f"{__name__}.AuditService")
        
        # In-memory storage (in production, use database)
        self._events: List[AuditEvent] = []
        
        # Configuration
        self._config = {
            'default_retention_days': 2555,  # 7 years
            'enable_audit_logging': True,
            'enable_security_events': True,
            'enable_compliance_tracking': True,
            'max_event_size': 10000,  # bytes
            'max_events_in_memory': 10000
        }
    
    # ==================== CONFIGURATION METHODS ====================
    
    def configure(self, **kwargs) -> None:
        """
        Configure audit service.
        
        Args:
            **kwargs: Configuration options
        """
        self._config.update(kwargs)
        self.logger.info(f"Audit service configured with: {kwargs}")
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.
        
        Args:
            key: Configuration key
            default: Default value
            
        Returns:
            Configuration value
        """
        return self._config.get(key, default)
    
    # ==================== AUDIT LOGGING METHODS ====================
    
    async def log_event(self, level: AuditLevel, category: AuditCategory, action: str,
                       description: str, user_id: str = None, session_id: str = None,
                       resource_type: str = None, resource_id: str = None,
                       details: Dict[str, Any] = None, ip_address: str = None,
                       user_agent: str = None, source: str = None, tags: List[str] = None,
                       status: AuditStatus = AuditStatus.SUCCESS) -> str:
        """
        Log an audit event.
        
        Args:
            level: Audit level
            category: Event category
            action: Action performed
            description: Event description
            user_id: User ID (if applicable)
            session_id: Session ID (if applicable)
            resource_type: Type of resource affected
            resource_id: ID of resource affected
            details: Additional event details
            ip_address: Client IP address
            user_agent: Client user agent
            source: Event source
            tags: Event tags
            status: Event status
            
        Returns:
            Event ID
        """
        try:
            if not self.get_config('enable_audit_logging', True):
                return ""
            
            # Generate event ID
            event_id = str(uuid.uuid4())
            
            # Create audit event
            event = AuditEvent(
                id=event_id,
                timestamp=datetime.utcnow(),
                level=level,
                category=category,
                status=status,
                user_id=user_id,
                session_id=session_id,
                action=action,
                resource_type=resource_type or "unknown",
                resource_id=resource_id,
                description=description,
                details=details or {},
                ip_address=ip_address,
                user_agent=user_agent,
                source=source or "audit_service",
                tags=tags or [],
                created_at=datetime.utcnow()
            )
            
            # Store event
            self._events.append(event)
            
            # Check memory limit
            if len(self._events) > self._config['max_events_in_memory']:
                self._events = self._events[-self._config['max_events_in_memory']//2:]
            
            self.logger.debug(f"Logged audit event: {event_id} - {action}")
            return event_id
            
        except Exception as e:
            self.logger.error(f"Error logging audit event: {str(e)}")
            return ""
    
    async def log_authentication(self, action: str, user_id: str = None, 
                               success: bool = True, details: Dict[str, Any] = None,
                               ip_address: str = None, user_agent: str = None) -> str:
        """Log authentication event."""
        return await self.log_event(
            level=AuditLevel.INFO if success else AuditLevel.WARNING,
            category=AuditCategory.AUTHENTICATION,
            action=action,
            description=f"Authentication {action}",
            user_id=user_id,
            details=details,
            ip_address=ip_address,
            user_agent=user_agent,
            status=AuditStatus.SUCCESS if success else AuditStatus.FAILURE,
            tags=["authentication", "security"]
        )
    
    async def log_data_access(self, action: str, user_id: str, resource_type: str,
                            resource_id: str = None, details: Dict[str, Any] = None) -> str:
        """Log data access event."""
        return await self.log_event(
            level=AuditLevel.INFO,
            category=AuditCategory.DATA_ACCESS,
            action=action,
            description=f"Data access {action}",
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
            tags=["data_access", "privacy"]
        )
    
    async def log_data_modification(self, action: str, user_id: str, resource_type: str,
                                  resource_id: str = None, details: Dict[str, Any] = None) -> str:
        """Log data modification event."""
        return await self.log_event(
            level=AuditLevel.INFO,
            category=AuditCategory.DATA_MODIFICATION,
            action=action,
            description=f"Data modification {action}",
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
            tags=["data_modification", "integrity"]
        )
    
    async def log_customer_event(self, action: str, user_id: str = None,
                                resource_id: str = None, details: Dict[str, Any] = None,
                                success: bool = True) -> str:
        """Log customer domain event."""
        return await self.log_event(
            level=AuditLevel.INFO if success else AuditLevel.ERROR,
            category=AuditCategory.CUSTOMER_EVENT,
            action=action,
            description=f"Customer {action}",
            user_id=user_id,
            resource_type="customer",
            resource_id=resource_id,
            details=details,
            status=AuditStatus.SUCCESS if success else AuditStatus.FAILURE,
            tags=["customer", "analytics"]
        )
    
    async def log_editorial_event(self, action: str, user_id: str = None,
                                  article_id: str = None, details: Dict[str, Any] = None,
                                  success: bool = True) -> str:
        """Log editorial domain event."""
        return await self.log_event(
            level=AuditLevel.INFO if success else AuditLevel.ERROR,
            category=AuditCategory.EDITORIAL_EVENT,
            action=action,
            description=f"Editorial {action}",
            user_id=user_id,
            resource_type="article",
            resource_id=article_id,
            details=details,
            status=AuditStatus.SUCCESS if success else AuditStatus.FAILURE,
            tags=["editorial", "content"]
        )
    
    async def log_company_event(self, action: str, user_id: str = None,
                               content_id: str = None, details: Dict[str, Any] = None,
                               success: bool = True) -> str:
        """Log company domain event."""
        return await self.log_event(
            level=AuditLevel.INFO if success else AuditLevel.ERROR,
            category=AuditCategory.COMPANY_EVENT,
            action=action,
            description=f"Company {action}",
            user_id=user_id,
            resource_type="internal_content",
            resource_id=content_id,
            details=details,
            status=AuditStatus.SUCCESS if success else AuditStatus.FAILURE,
            tags=["company", "internal"]
        )
    
    async def log_content_event(self, action: str, user_id: str = None,
                               content_id: str = None, details: Dict[str, Any] = None,
                               success: bool = True) -> str:
        """Log content event."""
        return await self.log_event(
            level=AuditLevel.INFO if success else AuditLevel.ERROR,
            category=AuditCategory.CONTENT_EVENT,
            action=action,
            description=f"Content {action}",
            user_id=user_id,
            resource_type="content",
            resource_id=content_id,
            details=details,
            status=AuditStatus.SUCCESS if success else AuditStatus.FAILURE,
            tags=["content", "media"]
        )
    
    async def log_media_event(self, action: str, user_id: str = None,
                             media_id: str = None, details: Dict[str, Any] = None,
                             success: bool = True) -> str:
        """Log media event."""
        return await self.log_event(
            level=AuditLevel.INFO if success else AuditLevel.ERROR,
            category=AuditCategory.MEDIA_EVENT,
            action=action,
            description=f"Media {action}",
            user_id=user_id,
            resource_type="media",
            resource_id=media_id,
            details=details,
            status=AuditStatus.SUCCESS if success else AuditStatus.FAILURE,
            tags=["media", "asset"]
        )
    
    async def log_ml_model_event(self, action: str, user_id: str = None,
                                model_id: str = None, details: Dict[str, Any] = None,
                                success: bool = True) -> str:
        """Log ML model event."""
        return await self.log_event(
            level=AuditLevel.INFO if success else AuditLevel.ERROR,
            category=AuditCategory.ML_MODEL_EVENT,
            action=action,
            description=f"ML Model {action}",
            user_id=user_id,
            resource_type="ml_model",
            resource_id=model_id,
            details=details,
            status=AuditStatus.SUCCESS if success else AuditStatus.FAILURE,
            tags=["ml", "model"]
        )
    
    async def log_data_quality_event(self, action: str, user_id: str = None,
                                     rule_id: str = None, details: Dict[str, Any] = None,
                                     success: bool = True) -> str:
        """Log data quality event."""
        return await self.log_event(
            level=AuditLevel.INFO if success else AuditLevel.ERROR,
            category=AuditCategory.DATA_QUALITY_EVENT,
            action=action,
            description=f"Data Quality {action}",
            user_id=user_id,
            resource_type="quality_rule",
            resource_id=rule_id,
            details=details,
            status=AuditStatus.SUCCESS if success else AuditStatus.FAILURE,
            tags=["data_quality", "validation"]
        )
    
    # ==================== QUERY METHODS ====================
    
    async def get_events(self, user_id: str = None, category: AuditCategory = None,
                        start_date: datetime = None, end_date: datetime = None,
                        limit: int = 100) -> List[AuditEvent]:
        """
        Get audit events matching criteria.
        
        Args:
            user_id: Filter by user ID
            category: Filter by category
            start_date: Filter events from this date
            end_date: Filter events until this date
            limit: Maximum number of events to return
            
        Returns:
            List of audit events
        """
        try:
            filtered_events = self._events.copy()
            
            # Apply filters
            if user_id:
                filtered_events = [e for e in filtered_events if e.user_id == user_id]
            
            if category:
                filtered_events = [e for e in filtered_events if e.category == category]
            
            if start_date:
                filtered_events = [e for e in filtered_events if e.timestamp >= start_date]
            
            if end_date:
                filtered_events = [e for e in filtered_events if e.timestamp <= end_date]
            
            # Sort by timestamp (newest first) and limit
            filtered_events.sort(key=lambda e: e.timestamp, reverse=True)
            return filtered_events[:limit]
            
        except Exception as e:
            self.logger.error(f"Error getting audit events: {str(e)}")
            return []
    
    async def get_event_by_id(self, event_id: str) -> Optional[AuditEvent]:
        """
        Get audit event by ID.
        
        Args:
            event_id: Event ID
            
        Returns:
            Audit event or None if not found
        """
        try:
            for event in self._events:
                if event.id == event_id:
                    return event
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting audit event: {str(e)}")
            return None
    
    # ==================== UTILITY METHODS ====================
    
    async def cleanup_old_events(self, retention_days: int = None) -> int:
        """
        Clean up old audit events.
        
        Args:
            retention_days: Number of days to retain events
            
        Returns:
            Number of events cleaned up
        """
        try:
            retention_days = retention_days or self._config['default_retention_days']
            cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
            
            initial_count = len(self._events)
            self._events = [e for e in self._events if e.timestamp >= cutoff_date]
            cleaned_count = initial_count - len(self._events)
            
            self.logger.info(f"Cleaned up {cleaned_count} old audit events")
            return cleaned_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning up audit events: {str(e)}")
            return 0
    
    def get_audit_summary(self) -> Dict[str, Any]:
        """
        Get audit service summary.
        
        Returns:
            Dictionary containing audit summary
        """
        return {
            "total_events": len(self._events),
            "config": self._config,
            "timestamp": datetime.utcnow().isoformat()
        }

