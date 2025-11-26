"""
OwnLens - Base Domain Models

Base reference tables: companies, brands, countries, cities, categories, users, user_accounts, devices.
"""

# Lazy imports to avoid circular import issues
# Models are imported on-demand using __getattr__

# Import base classes directly from the base.py file (not the base/ package)
# We need to import from the parent module's namespace
# Since there's a naming conflict (base.py file vs base/ package),
# we import the parent module and access its base attribute
import sys
import importlib

# Import the parent models module first
_parent_module_name = __name__.rsplit('.', 1)[0]  # 'src.ownlens.models'
_parent_module = sys.modules.get(_parent_module_name)
if _parent_module is None:
    _parent_module = importlib.import_module(_parent_module_name)

# Now import base.py explicitly (it will be available as _parent_module.base)
# But we need to make sure we're getting the file, not the package
# The file is imported as 'src.ownlens.models.base' but the package takes precedence
# So we need to import it with a different name first
import importlib.util
import os
_base_file_path = os.path.join(os.path.dirname(__file__), '..', 'base.py')
_base_file_path = os.path.abspath(_base_file_path)
_spec = importlib.util.spec_from_file_location('src.ownlens.models._base', _base_file_path)
_base_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_base_module)

BaseEntity = _base_module.BaseEntity
BaseSchema = _base_module.BaseSchema
TimestampMixin = _base_module.TimestampMixin
MetadataMixin = _base_module.MetadataMixin
CompanyBrandMixin = _base_module.CompanyBrandMixin
BaseEntityWithCompanyBrand = _base_module.BaseEntityWithCompanyBrand

__all__ = [
    # Base classes
    "BaseEntity",
    "BaseSchema",
    "TimestampMixin",
    "MetadataMixin",
    "CompanyBrandMixin",
    "BaseEntityWithCompanyBrand",
    # Model classes
    "Company",
    "CompanyCreate",
    "CompanyUpdate",
    "Brand",
    "BrandCreate",
    "BrandUpdate",
    "BrandCountry",
    "BrandCountryCreate",
    "BrandCountryUpdate",
    "Country",
    "CountryCreate",
    "CountryUpdate",
    "City",
    "CityCreate",
    "CityUpdate",
    "Category",
    "CategoryCreate",
    "CategoryUpdate",
    "UserAccount",
    "UserAccountCreate",
    "UserAccountUpdate",
    "DeviceType",
    "DeviceTypeCreate",
    "DeviceTypeUpdate",
    "OperatingSystem",
    "OperatingSystemCreate",
    "OperatingSystemUpdate",
    "Browser",
    "BrowserCreate",
    "BrowserUpdate",
]

# Lazy import mapping
_MODELS = {
    "Company": ("company", "Company"),
    "CompanyCreate": ("company", "CompanyCreate"),
    "CompanyUpdate": ("company", "CompanyUpdate"),
    "Brand": ("brand", "Brand"),
    "BrandCreate": ("brand", "BrandCreate"),
    "BrandUpdate": ("brand", "BrandUpdate"),
    "BrandCountry": ("brand_country", "BrandCountry"),
    "BrandCountryCreate": ("brand_country", "BrandCountryCreate"),
    "BrandCountryUpdate": ("brand_country", "BrandCountryUpdate"),
    "Country": ("country", "Country"),
    "CountryCreate": ("country", "CountryCreate"),
    "CountryUpdate": ("country", "CountryUpdate"),
    "City": ("city", "City"),
    "CityCreate": ("city", "CityCreate"),
    "CityUpdate": ("city", "CityUpdate"),
    "Category": ("category", "Category"),
    "CategoryCreate": ("category", "CategoryCreate"),
    "CategoryUpdate": ("category", "CategoryUpdate"),
    "UserAccount": ("user_account", "UserAccount"),
    "UserAccountCreate": ("user_account", "UserAccountCreate"),
    "UserAccountUpdate": ("user_account", "UserAccountUpdate"),
    "DeviceType": ("device_type", "DeviceType"),
    "DeviceTypeCreate": ("device_type", "DeviceTypeCreate"),
    "DeviceTypeUpdate": ("device_type", "DeviceTypeUpdate"),
    "OperatingSystem": ("operating_system", "OperatingSystem"),
    "OperatingSystemCreate": ("operating_system", "OperatingSystemCreate"),
    "OperatingSystemUpdate": ("operating_system", "OperatingSystemUpdate"),
    "Browser": ("browser", "Browser"),
    "BrowserCreate": ("browser", "BrowserCreate"),
    "BrowserUpdate": ("browser", "BrowserUpdate"),
}


def __getattr__(name: str):
    """Lazy import models to avoid circular imports."""
    if name in _MODELS:
        module_name, class_name = _MODELS[name]
        module = __import__(f".{module_name}", fromlist=[class_name], level=1)
        return getattr(module, class_name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

