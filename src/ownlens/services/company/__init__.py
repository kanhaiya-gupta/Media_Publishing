"""
OwnLens - Company Domain Services

Services for company domain entities (departments, employees, internal content, performance).
"""

from .department_service import DepartmentService
from .employee_service import EmployeeService
from .internal_content_service import InternalContentService

__all__ = [
    "DepartmentService",
    "EmployeeService",
    "InternalContentService",
]

