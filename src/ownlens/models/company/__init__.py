"""
OwnLens - Company Domain Models

Company analytics: internal content, employee engagement, department analytics.
"""

from .department import Department, DepartmentCreate, DepartmentUpdate
from .employee import Employee, EmployeeCreate, EmployeeUpdate
from .internal_content import InternalContent, InternalContentCreate, InternalContentUpdate
from .content_performance import ContentPerformance, ContentPerformanceCreate, ContentPerformanceUpdate
from .department_performance import DepartmentPerformance, DepartmentPerformanceCreate, DepartmentPerformanceUpdate
from .employee_engagement import EmployeeEngagement, EmployeeEngagementCreate, EmployeeEngagementUpdate
from .communications_analytics import CommunicationsAnalytics, CommunicationsAnalyticsCreate, CommunicationsAnalyticsUpdate
from .content_event import ContentEvent, ContentEventCreate, ContentEventUpdate

__all__ = [
    "Department",
    "DepartmentCreate",
    "DepartmentUpdate",
    "Employee",
    "EmployeeCreate",
    "EmployeeUpdate",
    "InternalContent",
    "InternalContentCreate",
    "InternalContentUpdate",
    "ContentPerformance",
    "ContentPerformanceCreate",
    "ContentPerformanceUpdate",
    "DepartmentPerformance",
    "DepartmentPerformanceCreate",
    "DepartmentPerformanceUpdate",
    "EmployeeEngagement",
    "EmployeeEngagementCreate",
    "EmployeeEngagementUpdate",
    "CommunicationsAnalytics",
    "CommunicationsAnalyticsCreate",
    "CommunicationsAnalyticsUpdate",
    "ContentEvent",
    "ContentEventCreate",
    "ContentEventUpdate",
]

