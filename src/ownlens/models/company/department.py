"""
OwnLens - Company Domain: Department Schema

Department validation models.
"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class DepartmentBase(BaseEntityWithCompanyBrand):
    """Base department schema"""

    department_id: UUID = Field(description="Department ID")
    department_name: str = Field(max_length=255, description="Department name")
    department_code: str = Field(max_length=100, description="Department code (unique within company)")
    parent_department_id: Optional[UUID] = Field(default=None, description="Parent department ID")
    description: Optional[str] = Field(default=None, description="Department description")

    # Geographic information
    primary_country_code: Optional[str] = Field(default=None, max_length=2, description="Primary country code")
    primary_city_id: Optional[UUID] = Field(default=None, description="Primary city ID")
    office_address: Optional[str] = Field(default=None, description="Office address")

    # Department metrics
    employee_count: int = Field(default=0, ge=0, description="Employee count")
    budget: Optional[float] = Field(default=None, ge=0, description="Budget")

    # Status
    is_active: bool = Field(default=True, description="Is department active")
    established_date: Optional[date] = Field(default=None, description="Established date")

    @field_validator("department_code")
    @classmethod
    def validate_department_code(cls, v: str) -> str:
        """Validate department code format"""
        return FieldValidators.validate_code(v, "department_code")

    @field_validator("primary_country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)


class Department(DepartmentBase):
    """Department schema (read)"""

    pass


class DepartmentCreate(DepartmentBase):
    """Department creation schema"""

    department_id: Optional[UUID] = Field(default=None, description="Department ID (auto-generated if not provided)")

    @field_validator("department_id", mode="before")
    @classmethod
    def generate_department_id(cls, v: Optional[UUID]) -> UUID:
        """Generate department ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "department_id")


class DepartmentUpdate(BaseEntityWithCompanyBrand):
    """Department update schema"""

    department_name: Optional[str] = Field(default=None, max_length=255, description="Department name")
    description: Optional[str] = Field(default=None, description="Department description")
    primary_country_code: Optional[str] = Field(default=None, max_length=2, description="Primary country code")
    primary_city_id: Optional[UUID] = Field(default=None, description="Primary city ID")
    office_address: Optional[str] = Field(default=None, description="Office address")
    employee_count: Optional[int] = Field(default=None, ge=0, description="Employee count")
    budget: Optional[float] = Field(default=None, ge=0, description="Budget")
    is_active: Optional[bool] = Field(default=None, description="Is department active")
    established_date: Optional[date] = Field(default=None, description="Established date")

    @field_validator("primary_country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)
