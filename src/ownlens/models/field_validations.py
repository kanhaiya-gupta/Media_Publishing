"""
OwnLens - Field Validations

Common field validators and validation utilities.
"""

import re
from datetime import datetime, date
from typing import Any, Optional
from uuid import UUID

from pydantic import field_validator, FieldValidationInfo


class FieldValidators:
    """Common field validators"""

    @staticmethod
    def validate_uuid(value: Any, field_name: str = "id") -> Optional[UUID]:
        """Validate UUID format"""
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return UUID(value)
            except ValueError:
                raise ValueError(f"{field_name}: Invalid UUID format: {value}")
        if isinstance(value, UUID):
            return value
        raise ValueError(f"{field_name}: Invalid UUID type: {type(value)}")

    @staticmethod
    def validate_email(value: Optional[str]) -> Optional[str]:
        """Validate email format"""
        if value is None or value == "":
            return None
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if not re.match(email_pattern, value):
            raise ValueError(f"Invalid email format: {value}")
        return value.lower().strip()

    @staticmethod
    def validate_url(value: Optional[str]) -> Optional[str]:
        """Validate URL format"""
        if value is None or value == "":
            return None
        url_pattern = r"^https?://[^\s/$.?#].[^\s]*$"
        if not re.match(url_pattern, value):
            raise ValueError(f"Invalid URL format: {value}")
        return value.strip()

    @staticmethod
    def validate_country_code(value: Optional[str]) -> Optional[str]:
        """Validate ISO 3166-1 alpha-2 country code"""
        if value is None or value == "":
            return None
        if not re.match(r"^[A-Z]{2}$", value):
            raise ValueError(f"Invalid country code format (must be ISO 3166-1 alpha-2): {value}")
        return value.upper()

    @staticmethod
    def validate_language_code(value: Optional[str]) -> Optional[str]:
        """Validate ISO 639-1 language code"""
        if value is None or value == "":
            return None
        # Strip whitespace before validation
        value = value.strip()
        if not re.match(r"^[a-z]{2}(-[A-Z]{2})?$", value):
            raise ValueError(f"Invalid language code format (must be ISO 639-1): {value}")
        return value.lower()

    @staticmethod
    def validate_timezone(value: Optional[str]) -> Optional[str]:
        """Validate timezone format"""
        if value is None or value == "":
            return None
        # Basic timezone validation - supports timezones with multiple slashes (e.g., America/Argentina/Buenos_Aires)
        # Pattern: one or more segments separated by slashes, each segment starts with letter/underscore and can contain letters, numbers, underscores, and hyphens
        # Format: Segment1/Segment2/Segment3/...
        timezone_pattern = r"^[A-Za-z_][A-Za-z0-9_-]*(/[A-Za-z_][A-Za-z0-9_-]*)+$"
        if not re.match(timezone_pattern, value):
            raise ValueError(f"Invalid timezone format: {value}")
        return value

    @staticmethod
    def validate_phone_number(value: Optional[str]) -> Optional[str]:
        """Validate phone number format"""
        if value is None or value == "":
            return None
        # Basic phone validation (can be enhanced for international formats)
        phone_pattern = r"^\+?[1-9]\d{1,14}$"
        cleaned = re.sub(r"[\s\-\(\)]", "", value)
        if not re.match(phone_pattern, cleaned):
            raise ValueError(f"Invalid phone number format: {value}")
        return cleaned

    @staticmethod
    def validate_date(value: Any) -> Optional[date]:
        """Validate date format"""
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).date()
            except ValueError:
                try:
                    return datetime.strptime(value, "%Y-%m-%d").date()
                except ValueError:
                    raise ValueError(f"Invalid date format: {value}")
        raise ValueError(f"Invalid date type: {type(value)}")

    @staticmethod
    def validate_datetime(value: Any) -> Optional[datetime]:
        """Validate datetime format"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                try:
                    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    raise ValueError(f"Invalid datetime format: {value}")
        raise ValueError(f"Invalid datetime type: {type(value)}")

    @staticmethod
    def validate_timestamp(value: Any) -> Optional[int]:
        """Validate Unix timestamp"""
        if value is None:
            return None
        if isinstance(value, int):
            if value < 0:
                raise ValueError(f"Invalid timestamp (must be positive): {value}")
            return value
        if isinstance(value, float):
            return int(value)
        raise ValueError(f"Invalid timestamp type: {type(value)}")

    @staticmethod
    def validate_percentage(value: Optional[float], min_value: float = 0.0, max_value: float = 1.0) -> Optional[float]:
        """Validate percentage (0.0 to 1.0)"""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            raise ValueError(f"Invalid percentage type: {type(value)}")
        if value < min_value or value > max_value:
            raise ValueError(f"Percentage must be between {min_value} and {max_value}: {value}")
        return float(value)

    @staticmethod
    def validate_positive_integer(value: Optional[int], field_name: str = "value") -> Optional[int]:
        """Validate positive integer"""
        if value is None:
            return None
        if not isinstance(value, int):
            raise ValueError(f"{field_name}: Must be an integer")
        if value < 0:
            raise ValueError(f"{field_name}: Must be positive or zero")
        return value

    @staticmethod
    def validate_non_negative_integer(value: Optional[int], field_name: str = "value") -> Optional[int]:
        """Validate non-negative integer"""
        if value is None:
            return None
        if not isinstance(value, int):
            raise ValueError(f"{field_name}: Must be an integer")
        if value < 0:
            raise ValueError(f"{field_name}: Must be non-negative")
        return value

    @staticmethod
    def validate_positive_float(value: Optional[float], field_name: str = "value") -> Optional[float]:
        """Validate positive float"""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            raise ValueError(f"{field_name}: Must be a number")
        if value < 0:
            raise ValueError(f"{field_name}: Must be positive or zero")
        return float(value)

    @staticmethod
    def validate_string_length(value: Optional[str], min_length: int = 0, max_length: int = None, field_name: str = "value") -> Optional[str]:
        """Validate string length"""
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"{field_name}: Must be a string")
        if len(value) < min_length:
            raise ValueError(f"{field_name}: Must be at least {min_length} characters")
        if max_length is not None and len(value) > max_length:
            raise ValueError(f"{field_name}: Must be at most {max_length} characters")
        return value.strip() if value else value

    @staticmethod
    def validate_code(value: Optional[str], field_name: str = "code") -> Optional[str]:
        """Validate code format (alphanumeric, underscores, hyphens)"""
        if value is None or value == "":
            return None
        if not re.match(r"^[a-zA-Z0-9_-]+$", value):
            raise ValueError(f"{field_name}: Must contain only alphanumeric characters, underscores, and hyphens")
        return value.lower().strip()

    @staticmethod
    def validate_slug(value: Optional[str], field_name: str = "slug") -> Optional[str]:
        """Validate slug format (lowercase, hyphens)"""
        if value is None or value == "":
            return None
        if not re.match(r"^[a-z0-9-]+$", value):
            raise ValueError(f"{field_name}: Must contain only lowercase letters, numbers, and hyphens")
        return value.lower().strip()

    @staticmethod
    def validate_enum(value: Any, allowed_values: list, field_name: str = "value") -> Any:
        """Validate enum value"""
        if value is None:
            return None
        if value not in allowed_values:
            raise ValueError(f"{field_name}: Must be one of {allowed_values}, got {value}")
        return value

    @staticmethod
    def validate_array(value: Optional[list], min_items: int = 0, max_items: int = None, field_name: str = "value") -> Optional[list]:
        """Validate array length"""
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError(f"{field_name}: Must be a list")
        if len(value) < min_items:
            raise ValueError(f"{field_name}: Must have at least {min_items} items")
        if max_items is not None and len(value) > max_items:
            raise ValueError(f"{field_name}: Must have at most {max_items} items")
        return value

    @staticmethod
    def validate_json(value: Optional[dict], field_name: str = "value") -> Optional[dict]:
        """Validate JSON object"""
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError(f"{field_name}: Must be a dictionary")
        return value

    @staticmethod
    def validate_file_extension(value: Optional[str], allowed_extensions: list = None) -> Optional[str]:
        """Validate file extension"""
        if value is None or value == "":
            return None
        if allowed_extensions is None:
            allowed_extensions = ["jpg", "jpeg", "png", "gif", "webp", "mp4", "pdf", "doc", "docx"]
        ext = value.lower().lstrip(".")
        if ext not in allowed_extensions:
            raise ValueError(f"Invalid file extension: {value}. Allowed: {allowed_extensions}")
        return ext

    @staticmethod
    def validate_mime_type(value: Optional[str], allowed_types: list = None) -> Optional[str]:
        """Validate MIME type"""
        if value is None or value == "":
            return None
        if allowed_types is None:
            allowed_types = [
                "image/jpeg", "image/png", "image/gif", "image/webp",
                "video/mp4", "video/webm",
                "application/pdf", "application/msword", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            ]
        if value not in allowed_types:
            raise ValueError(f"Invalid MIME type: {value}. Allowed: {allowed_types}")
        return value.lower()

    @staticmethod
    def validate_hex_color(value: Optional[str]) -> Optional[str]:
        """Validate hex color code"""
        if value is None or value == "":
            return None
        if not re.match(r"^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$", value):
            raise ValueError(f"Invalid hex color format: {value}")
        return value.upper()

    @staticmethod
    def validate_ip_address(value: Optional[str]) -> Optional[str]:
        """Validate IP address (IPv4 or IPv6)"""
        if value is None or value == "":
            return None
        # Basic IP validation (can be enhanced)
        ipv4_pattern = r"^(\d{1,3}\.){3}\d{1,3}$"
        ipv6_pattern = r"^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
        if not (re.match(ipv4_pattern, value) or re.match(ipv6_pattern, value)):
            raise ValueError(f"Invalid IP address format: {value}")
        return value

    @staticmethod
    def validate_user_agent(value: Optional[str]) -> Optional[str]:
        """Validate user agent string"""
        if value is None or value == "":
            return None
        if len(value) > 500:
            raise ValueError(f"User agent string too long (max 500 characters): {len(value)}")
        return value

    @staticmethod
    def validate_status(value: Optional[str], allowed_statuses: list, field_name: str = "status") -> Optional[str]:
        """Validate status value"""
        if value is None:
            return None
        if value not in allowed_statuses:
            raise ValueError(f"{field_name}: Must be one of {allowed_statuses}, got {value}")
        return value.lower()

    @staticmethod
    def validate_boolean(value: Any) -> bool:
        """Validate boolean value"""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ["true", "1", "yes", "on"]
        if isinstance(value, int):
            return value != 0
        raise ValueError(f"Invalid boolean value: {value}")

    @staticmethod
    def validate_decimal(value: Optional[float], precision: int = 2, field_name: str = "value") -> Optional[float]:
        """Validate decimal with precision"""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            raise ValueError(f"{field_name}: Must be a number")
        return round(float(value), precision)

    @staticmethod
    def validate_currency_code(value: Optional[str]) -> Optional[str]:
        """Validate ISO 4217 currency code"""
        if value is None or value == "":
            return None
        if not re.match(r"^[A-Z]{3}$", value):
            raise ValueError(f"Invalid currency code format (must be ISO 4217): {value}")
        return value.upper()

    @staticmethod
    def validate_iso_date(value: Any) -> Optional[date]:
        """Validate ISO 8601 date format"""
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).date()
            except ValueError:
                raise ValueError(f"Invalid ISO date format: {value}")
        raise ValueError(f"Invalid date type: {type(value)}")

    @staticmethod
    def validate_iso_datetime(value: Any) -> Optional[datetime]:
        """Validate ISO 8601 datetime format"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                # Handle Z suffix and timezone
                value = value.replace("Z", "+00:00")
                return datetime.fromisoformat(value)
            except ValueError:
                raise ValueError(f"Invalid ISO datetime format: {value}")
        raise ValueError(f"Invalid datetime type: {type(value)}")


# Convenience functions for common validators
def validate_uuid(value: Any, field_name: str = "id") -> Optional[UUID]:
    """Validate UUID format"""
    return FieldValidators.validate_uuid(value, field_name)


def validate_email(value: Optional[str]) -> Optional[str]:
    """Validate email format"""
    return FieldValidators.validate_email(value)


def validate_url(value: Optional[str]) -> Optional[str]:
    """Validate URL format"""
    return FieldValidators.validate_url(value)


def validate_country_code(value: Optional[str]) -> Optional[str]:
    """Validate ISO 3166-1 alpha-2 country code"""
    return FieldValidators.validate_country_code(value)


def validate_language_code(value: Optional[str]) -> Optional[str]:
    """Validate ISO 639-1 language code"""
    return FieldValidators.validate_language_code(value)

