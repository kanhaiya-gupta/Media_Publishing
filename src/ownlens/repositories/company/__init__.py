"""
OwnLens - Company Domain Repositories

Repositories for company domain entities (departments, employees, internal content, performance, engagement, analytics, events).
"""

from .department_repository import DepartmentRepository
from .employee_repository import EmployeeRepository
from .internal_content_repository import InternalContentRepository
from .content_performance_repository import ContentPerformanceRepository
from .department_performance_repository import DepartmentPerformanceRepository
from .employee_engagement_repository import EmployeeEngagementRepository
from .communications_analytics_repository import CommunicationsAnalyticsRepository
from .content_event_repository import ContentEventRepository

__all__ = [
    "DepartmentRepository",
    "EmployeeRepository",
    "InternalContentRepository",
    "ContentPerformanceRepository",
    "DepartmentPerformanceRepository",
    "EmployeeEngagementRepository",
    "CommunicationsAnalyticsRepository",
    "ContentEventRepository",
]

