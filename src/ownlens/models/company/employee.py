"""
OwnLens - Company Domain: Employee Schema

Employee validation models.
"""

from datetime import date
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class EmployeeBase(BaseEntityWithCompanyBrand):
    """Base employee schema"""

    employee_id: UUID = Field(description="Employee ID")
    user_id: UUID = Field(description="User ID (required - links to users table)")

    # Employee information
    employee_number: Optional[str] = Field(default=None, max_length=100, description="Employee number")
    job_title: Optional[str] = Field(default=None, max_length=255, description="Job title")
    job_level: Optional[str] = Field(default=None, max_length=50, description="Job level")
    employment_type: Optional[str] = Field(default=None, max_length=50, description="Employment type")

    # Department and role
    department_id: Optional[UUID] = Field(default=None, description="Department ID")
    manager_id: Optional[UUID] = Field(default=None, description="Manager ID (employee_id)")

    # Employment information
    hire_date: date = Field(description="Hire date (required)")
    termination_date: Optional[date] = Field(default=None, description="Termination date")

    # Status
    is_active: bool = Field(default=True, description="Is employee active")

    @field_validator("job_level")
    @classmethod
    def validate_job_level(cls, v: Optional[str]) -> Optional[str]:
        """Validate job level"""
        if v is None:
            return None
        allowed_levels = ["entry", "junior", "mid", "senior", "lead", "manager", "director", "executive"]
        return FieldValidators.validate_enum(v.lower(), allowed_levels, "job_level")

    @field_validator("employment_type")
    @classmethod
    def validate_employment_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate employment type"""
        if v is None:
            return None
        allowed_types = ["full_time", "part_time", "contract", "intern"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "employment_type")


class Employee(EmployeeBase):
    """Employee schema (read)"""

    pass


class EmployeeCreate(EmployeeBase):
    """Employee creation schema"""

    employee_id: Optional[UUID] = Field(default=None, description="Employee ID (auto-generated if not provided)")

    @field_validator("employee_id", mode="before")
    @classmethod
    def generate_employee_id(cls, v: Optional[UUID]) -> UUID:
        """Generate employee ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "employee_id")


class EmployeeUpdate(BaseEntityWithCompanyBrand):
    """Employee update schema"""

    department_id: Optional[UUID] = Field(default=None, description="Department ID")
    job_title: Optional[str] = Field(default=None, max_length=255, description="Job title")
    job_level: Optional[str] = Field(default=None, max_length=50, description="Job level")
    manager_id: Optional[UUID] = Field(default=None, description="Manager ID")
    termination_date: Optional[date] = Field(default=None, description="Termination date")
    is_active: Optional[bool] = Field(default=None, description="Is employee active")

