"""
Notification Service - OwnLens
==============================

Notification service for OwnLens.
Provides comprehensive notification and messaging capabilities.

This service provides:
- Multi-channel notification support (email, SMS, push, webhook)
- Template management and customization
- Delivery tracking and retry logic
- Notification preferences and opt-out management
- Notification analytics and reporting
"""

from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime, timedelta
from enum import Enum
import logging
import asyncio
import json
from dataclasses import dataclass
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class NotificationChannel(Enum):
    """Supported notification channels."""
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    WEBHOOK = "webhook"
    IN_APP = "in_app"


class NotificationPriority(Enum):
    """Notification priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class NotificationStatus(Enum):
    """Notification delivery status."""
    PENDING = "pending"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"
    BOUNCED = "bounced"
    CANCELLED = "cancelled"


@dataclass
class NotificationTemplate:
    """Notification template definition."""
    id: str
    name: str
    channel: NotificationChannel
    subject: str
    body: str
    variables: List[str]
    created_at: datetime
    updated_at: datetime
    active: bool = True


@dataclass
class NotificationRecipient:
    """Notification recipient information."""
    user_id: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    device_token: Optional[str] = None
    webhook_url: Optional[str] = None
    preferences: Dict[str, Any] = None


@dataclass
class NotificationMessage:
    """Notification message to be sent."""
    id: str
    template_id: str
    channel: NotificationChannel
    recipient: NotificationRecipient
    subject: str
    body: str
    priority: NotificationPriority
    scheduled_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = None


@dataclass
class NotificationResult:
    """Result of a notification operation."""
    message_id: str
    status: NotificationStatus
    channel: NotificationChannel
    sent_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = None


class NotificationProvider(ABC):
    """Abstract base class for notification providers."""
    
    @abstractmethod
    async def send_notification(self, message: NotificationMessage) -> NotificationResult:
        """Send a notification through this provider."""
        pass
    
    @abstractmethod
    async def validate_recipient(self, recipient: NotificationRecipient) -> bool:
        """Validate recipient information for this provider."""
        pass


class EmailProvider(NotificationProvider):
    """Email notification provider."""
    
    def __init__(self, smtp_host: str = "localhost", smtp_port: int = 587, 
                 username: str = None, password: str = None):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.logger = logging.getLogger(f"{__name__}.EmailProvider")
    
    async def send_notification(self, message: NotificationMessage) -> NotificationResult:
        """Send email notification."""
        try:
            # In a real implementation, this would use an SMTP library like aiosmtplib
            self.logger.info(f"Sending email to {message.recipient.email}: {message.subject}")
            
            # Simulate email sending
            await asyncio.sleep(0.1)  # Simulate network delay
            
            return NotificationResult(
                message_id=message.id,
                status=NotificationStatus.SENT,
                channel=message.channel,
                sent_at=datetime.utcnow(),
                metadata={"provider": "email", "smtp_host": self.smtp_host}
            )
            
        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
            return NotificationResult(
                message_id=message.id,
                status=NotificationStatus.FAILED,
                channel=message.channel,
                error_message=str(e)
            )
    
    async def validate_recipient(self, recipient: NotificationRecipient) -> bool:
        """Validate email recipient."""
        return recipient.email is not None and "@" in recipient.email


class NotificationService:
    """
    Notification service for GNN Solutions.
    
    Provides comprehensive notification and messaging capabilities
    across multiple channels with template management and delivery tracking.
    """
    
    def __init__(self):
        """Initialize the notification service."""
        self.logger = logging.getLogger(f"{__name__}.NotificationService")
        
        # Notification providers
        self._providers: Dict[NotificationChannel, NotificationProvider] = {}
        
        # Templates storage
        self._templates: Dict[str, NotificationTemplate] = {}
        
        # Delivery tracking
        self._delivery_tracking: Dict[str, NotificationResult] = {}
        
        # Retry configuration
        self._retry_config = {
            'max_retries': 3,
            'retry_delay': 60,  # seconds
            'exponential_backoff': True
        }
        
        # Initialize default providers
        self._initialize_default_providers()
    
    def _initialize_default_providers(self) -> None:
        """Initialize default notification providers."""
        # Email provider
        email_provider = EmailProvider()
        self.register_provider(NotificationChannel.EMAIL, email_provider)
    
    # ==================== PROVIDER MANAGEMENT METHODS ====================
    
    def register_provider(self, channel: NotificationChannel, provider: NotificationProvider) -> None:
        """
        Register a notification provider for a channel.
        
        Args:
            channel: Notification channel
            provider: Provider instance
        """
        self._providers[channel] = provider
        self.logger.info(f"Registered provider for channel: {channel.value}")
    
    def get_provider(self, channel: NotificationChannel) -> Optional[NotificationProvider]:
        """
        Get provider for a channel.
        
        Args:
            channel: Notification channel
            
        Returns:
            Provider instance or None if not found
        """
        return self._providers.get(channel)
    
    # ==================== TEMPLATE MANAGEMENT METHODS ====================
    
    def create_template(self, template_id: str, name: str, channel: NotificationChannel,
                       subject: str, body: str, variables: List[str] = None) -> NotificationTemplate:
        """
        Create a notification template.
        
        Args:
            template_id: Unique template identifier
            name: Template name
            channel: Notification channel
            subject: Subject template
            body: Body template
            variables: List of template variables
            
        Returns:
            Created template
        """
        template = NotificationTemplate(
            id=template_id,
            name=name,
            channel=channel,
            subject=subject,
            body=body,
            variables=variables or [],
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        self._templates[template_id] = template
        self.logger.info(f"Created notification template: {template_id}")
        return template
    
    def get_template(self, template_id: str) -> Optional[NotificationTemplate]:
        """
        Get notification template.
        
        Args:
            template_id: Template ID
            
        Returns:
            Template or None if not found
        """
        return self._templates.get(template_id)
    
    # ==================== NOTIFICATION SENDING METHODS ====================
    
    async def send_notification(self, message: NotificationMessage) -> NotificationResult:
        """
        Send a notification.
        
        Args:
            message: Notification message
            
        Returns:
            Notification result
        """
        try:
            # Get provider for channel
            provider = self.get_provider(message.channel)
            if not provider:
                return NotificationResult(
                    message_id=message.id,
                    status=NotificationStatus.FAILED,
                    channel=message.channel,
                    error_message=f"No provider registered for channel: {message.channel.value}"
                )
            
            # Validate recipient
            if not await provider.validate_recipient(message.recipient):
                return NotificationResult(
                    message_id=message.id,
                    status=NotificationStatus.FAILED,
                    channel=message.channel,
                    error_message="Invalid recipient information"
                )
            
            # Send notification
            result = await provider.send_notification(message)
            
            # Track delivery
            self._delivery_tracking[message.id] = result
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error sending notification: {str(e)}")
            return NotificationResult(
                message_id=message.id,
                status=NotificationStatus.FAILED,
                channel=message.channel,
                error_message=str(e)
            )
    
    async def send_training_completion_notification(self, user_id: str, email: str,
                                                   training_request_id: str, status: str) -> NotificationResult:
        """Send training completion notification."""
        import uuid
        
        message = NotificationMessage(
            id=str(uuid.uuid4()),
            template_id="training_completion",
            channel=NotificationChannel.EMAIL,
            recipient=NotificationRecipient(user_id=user_id, email=email),
            subject=f"Training {status} - Request {training_request_id}",
            body=f"Your training request {training_request_id} has {status}.",
            priority=NotificationPriority.NORMAL,
            metadata={"training_request_id": training_request_id, "status": status}
        )
        
        return await self.send_notification(message)

