"""
OwnLens - Base Domain Repositories

Repositories for base domain entities (companies, brands, countries, users, etc.).
"""

from .company_repository import CompanyRepository
from .brand_repository import BrandRepository
from .brand_country_repository import BrandCountryRepository
from .country_repository import CountryRepository
from .city_repository import CityRepository
from .category_repository import CategoryRepository
from .user_repository import UserRepository
from .user_account_repository import UserAccountRepository
from .device_type_repository import DeviceTypeRepository
from .operating_system_repository import OperatingSystemRepository
from .browser_repository import BrowserRepository

__all__ = [
    "CompanyRepository",
    "BrandRepository",
    "BrandCountryRepository",
    "CountryRepository",
    "CityRepository",
    "CategoryRepository",
    "UserRepository",
    "UserAccountRepository",
    "DeviceTypeRepository",
    "OperatingSystemRepository",
    "BrowserRepository",
]

