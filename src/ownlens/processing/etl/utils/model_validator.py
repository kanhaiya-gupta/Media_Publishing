"""
Pydantic Model Validator for ETL
=================================

Validates Spark DataFrame rows against Pydantic models.
"""

from typing import Type, List, Dict, Any, Optional, Tuple, Union, get_origin, get_args
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import BooleanType, StringType, DecimalType, TimestampType, DateType
from pydantic import BaseModel, ValidationError
from uuid import UUID
from datetime import date, datetime
from decimal import Decimal
import json
import logging

logger = logging.getLogger(__name__)


class ModelValidator:
    """
    Validates Spark DataFrame rows against Pydantic models.
    
    Provides methods to:
    - Validate DataFrame rows against Pydantic models
    - Filter valid/invalid rows
    - Get validation errors
    - Transform DataFrame to match model schema
    """
    
    @staticmethod
    def validate_dataframe(
        df: DataFrame,
        model_class: Type[BaseModel],
        strict: bool = False,
        filter_invalid: bool = True
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Validate DataFrame rows against Pydantic model.
        
        Args:
            df: Spark DataFrame to validate
            model_class: Pydantic model class to validate against
            strict: Whether to use strict validation
            filter_invalid: Whether to filter out invalid rows
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        try:
            # Get model fields to identify JSON fields (Dict/List types)
            model_fields = model_class.model_fields
            json_fields = set()
            
            for field_name, field_info in model_fields.items():
                field_type = field_info.annotation
                # Check if field is Dict or List type (including Optional[Dict] or Optional[List])
                origin = get_origin(field_type)
                
                # Handle Optional types
                if origin is Union or (hasattr(origin, '__origin__') and origin.__origin__ is Union):
                    args = get_args(field_type)
                    # Check if one of the args is Dict or List
                    for arg in args:
                        arg_origin = get_origin(arg) if hasattr(arg, '__origin__') else None
                        if arg_origin is dict or arg_origin is Dict or arg is dict or arg is Dict:
                            json_fields.add(field_name)
                            break
                        elif arg_origin is list or arg_origin is List or arg is list or arg is List:
                            json_fields.add(field_name)
                            break
                elif origin is dict or origin is Dict:
                    json_fields.add(field_name)
                elif origin is list or origin is List:
                    json_fields.add(field_name)
            
            # Convert DataFrame to RDD for row-by-row validation
            def validate_row(row: Row) -> Tuple[bool, Dict[str, Any], Optional[str]]:
                """Validate a single row against Pydantic model."""
                row_dict = row.asDict()  # Define outside try block
                
                # Parse JSON strings for JSON fields and handle missing/None values
                for field_name in json_fields:
                    # Get field info to determine expected type
                    field_info = model_fields.get(field_name)
                    if not field_info:
                        continue
                    
                    field_type = field_info.annotation
                    origin = get_origin(field_type)
                    
                    # Determine if field is Dict or List type (including Optional variants)
                    is_dict_type = False
                    is_list_type = False
                    
                    if origin is Union or (hasattr(origin, '__origin__') and origin.__origin__ is Union):
                        args = get_args(field_type)
                        for arg in args:
                            arg_origin = get_origin(arg) if hasattr(arg, '__origin__') else None
                            if arg_origin is dict or arg_origin is Dict or arg is dict or arg is Dict:
                                is_dict_type = True
                                break
                            elif arg_origin is list or arg_origin is List or arg is list or arg is List:
                                is_list_type = True
                                break
                    elif origin is dict or origin is Dict:
                        is_dict_type = True
                    elif origin is list or origin is List:
                        is_list_type = True
                    
                    # Handle missing field (not in row_dict)
                    if field_name not in row_dict:
                        # If field is expected to be Dict/List, set to empty dict/list
                        if is_dict_type:
                            row_dict[field_name] = {}
                        elif is_list_type:
                            row_dict[field_name] = []
                        continue
                    
                    value = row_dict[field_name]
                    
                    # Handle None values
                    if value is None:
                        # If field is expected to be Dict/List, set to empty dict/list
                        if is_dict_type:
                            row_dict[field_name] = {}
                        elif is_list_type:
                            row_dict[field_name] = []
                        continue
                    
                    # If value is a string, try to parse as JSON
                    if isinstance(value, str):
                        value_stripped = value.strip()
                        if value_stripped:
                            try:
                                row_dict[field_name] = json.loads(value_stripped)
                            except (json.JSONDecodeError, ValueError) as e:
                                # If JSON parsing fails, set to empty dict/list based on field type
                                if is_dict_type:
                                    row_dict[field_name] = {}
                                elif is_list_type:
                                    row_dict[field_name] = []
                        else:
                            # Empty string - set to empty dict/list based on field type
                            if is_dict_type:
                                row_dict[field_name] = {}
                            elif is_list_type:
                                row_dict[field_name] = []
                
                # Convert Spark types to Python types before validation
                def convert_spark_to_python(value: Any, field_name: str, field_info) -> Any:
                    """Convert Spark types to Python types for Pydantic validation."""
                    if value is None:
                        return None
                    
                    # Check if this is a Spark TimestampType value - it might be a datetime object
                    # but we need to ensure it's properly handled
                    # Spark TimestampType values are often returned as datetime objects from Row.asDict()
                    # but they might be timezone-naive
                    if hasattr(value, '__class__'):
                        class_name = value.__class__.__name__
                        # Check if it's a datetime-like object (including Spark's internal datetime types)
                        if 'datetime' in class_name.lower() or 'timestamp' in class_name.lower():
                            # Try to convert to Python datetime if it's not already
                            if not isinstance(value, datetime):
                                try:
                                    # Try to convert via string representation
                                    value_str = str(value)
                                    from dateutil import parser
                                    parsed_dt = parser.parse(value_str)
                                    if parsed_dt.tzinfo is None:
                                        from datetime import timezone
                                        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                    return parsed_dt
                                except Exception:
                                    pass
                    
                    # Get field type annotation
                    field_type = field_info.annotation if field_info else None
                    
                    # Helper to check if type matches
                    def is_type_match(expected_type):
                        """Check if field_type matches expected_type."""
                        if not field_type:
                            return False
                        origin = get_origin(field_type)
                        if origin is Union:
                            args = get_args(field_type)
                            # Check if expected_type is in args
                            for arg in args:
                                if arg is expected_type or (hasattr(arg, '__name__') and hasattr(expected_type, '__name__') and arg.__name__ == expected_type.__name__):
                                    return True
                            return False
                        # Direct type match
                        if field_type is expected_type:
                            return True
                        # Check by name (handles typing module types)
                        if hasattr(field_type, '__name__') and hasattr(expected_type, '__name__'):
                            return field_type.__name__ == expected_type.__name__
                        # Check if it's a class and matches
                        if isinstance(field_type, type) and isinstance(expected_type, type):
                            return field_type is expected_type
                        return False
                    
                    # Handle Decimal (from PostgreSQL numeric/decimal columns)
                    # PostgreSQL INTEGER columns are often read as Decimal by Spark
                    if isinstance(value, Decimal):
                        # Always try to convert Decimal to int/float
                        # First check if field expects int
                        if is_type_match(int):
                            return int(value)
                        # Check if field expects float
                        elif is_type_match(float):
                            return float(value)
                        # If field type is not clear, try to infer from field name
                        # Integer fields often have names like "count", "total", "views", etc.
                        elif any(keyword in field_name.lower() for keyword in ['count', 'total', 'views', 'clicks', 'likes', 'shares', 'comments', 'bookmarks', 'plays', 'completions', 'conversions', 'signups', 'rank', 'id', 'revenue']):
                            # Likely an integer field - convert to int
                            try:
                                return int(value)
                            except (ValueError, TypeError, OverflowError):
                                # If conversion fails, try float
                                return float(value)
                        # Default: convert to float
                        return float(value)
                    
                    # Handle UUID strings - check field type first
                    if isinstance(value, str):
                        if is_type_match(UUID):
                            try:
                                return UUID(value)
                            except (ValueError, AttributeError):
                                return value
                        # Also check if field name suggests UUID
                        elif (field_name.endswith('_id') or 'uuid' in field_name.lower()) and field_type:
                            origin = get_origin(field_type)
                            if origin is Union:
                                args = get_args(field_type)
                                if UUID in args:
                                    try:
                                        return UUID(value)
                                    except (ValueError, AttributeError):
                                        return value
                            elif field_type is UUID:
                                try:
                                    return UUID(value)
                                except (ValueError, AttributeError):
                                    return value
                    
                    # Handle datetime/date - check if field expects datetime/date
                    # Also check by field name (for fields from base classes like created_at, updated_at)
                    # Exclude "timezone" fields explicitly - they are strings, not datetime
                    is_datetime_field_by_name = (
                        (field_name.endswith('_at') or 
                         field_name.endswith('_timestamp') or 
                         field_name.endswith('_date')) and
                        'timezone' not in field_name.lower()
                    )
                    
                    # First check if value is already a datetime/date object
                    # Also check for datetime-like objects that might not be recognized as datetime
                    is_datetime_like = isinstance(value, datetime) or (
                        hasattr(value, '__class__') and 
                        ('datetime' in value.__class__.__name__.lower() or 
                         'timestamp' in value.__class__.__name__.lower() or
                         hasattr(value, 'year') and hasattr(value, 'month') and hasattr(value, 'day'))
                    )
                    
                    # If field name suggests datetime or field type expects datetime, handle it
                    if is_type_match(datetime) or is_datetime_field_by_name:
                        # Handle datetime conversion
                        if isinstance(value, datetime) or (is_datetime_like and not isinstance(value, datetime)):
                            # If it's datetime-like but not a datetime, try to convert
                            if not isinstance(value, datetime):
                                try:
                                    # Try to convert via string representation
                                    value_str = str(value)
                                    from dateutil import parser
                                    value = parser.parse(value_str)
                                except Exception:
                                    # If that fails, try to construct datetime from attributes
                                    try:
                                        if hasattr(value, 'year') and hasattr(value, 'month') and hasattr(value, 'day'):
                                            hour = getattr(value, 'hour', 0)
                                            minute = getattr(value, 'minute', 0)
                                            second = getattr(value, 'second', 0)
                                            microsecond = getattr(value, 'microsecond', 0)
                                            value = datetime(
                                                value.year, value.month, value.day,
                                                hour, minute, second, microsecond
                                            )
                                    except Exception:
                                        pass
                        
                        if isinstance(value, datetime):
                            # Already a datetime object - ensure it's timezone-aware if needed
                            # If datetime is timezone-naive, make it timezone-aware (assume UTC)
                            if value.tzinfo is None:
                                try:
                                    from datetime import timezone
                                    return value.replace(tzinfo=timezone.utc)
                                except Exception:
                                    # If that fails, return as-is and let Pydantic handle it
                                    return value
                            return value
                    
                    # Also check if it's already a datetime (even if field type doesn't match)
                    if isinstance(value, datetime):
                        # Already a datetime object - ensure it's timezone-aware if needed
                        if is_type_match(datetime) or is_datetime_field_by_name:
                            # If datetime is timezone-naive, make it timezone-aware (assume UTC)
                            if value.tzinfo is None:
                                try:
                                    from datetime import timezone
                                    return value.replace(tzinfo=timezone.utc)
                                except Exception:
                                    # If that fails, return as-is and let Pydantic handle it
                                    return value
                            return value
                        # If field expects date, extract date part
                        elif is_type_match(date):
                            return value.date()
                        return value
                    
                    if isinstance(value, date) and not isinstance(value, datetime):
                        # Already a date object
                        if is_type_match(date):
                            return value
                        # If field expects datetime, convert to datetime at midnight
                        elif is_type_match(datetime):
                            return datetime.combine(value, datetime.min.time())
                        return value
                    
                    # Handle datetime/date strings - check field type first, then by field name
                    if isinstance(value, str):
                        # Try to parse as datetime/date if field expects it (by type or name)
                        if is_type_match(datetime) or is_datetime_field_by_name:
                            try:
                                # Try parsing as ISO format first
                                if 'T' in value or ' ' in value:
                                    # ISO format or space-separated
                                    value_clean = value.replace('Z', '+00:00')
                                    try:
                                        parsed_dt = datetime.fromisoformat(value_clean)
                                        # Ensure timezone-aware if needed
                                        if parsed_dt.tzinfo is None:
                                            from datetime import timezone
                                            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                        return parsed_dt
                                    except ValueError:
                                        # Try without timezone
                                        if '+' in value_clean or value_clean.endswith('Z'):
                                            # Remove timezone info and try again
                                            value_no_tz = value_clean.split('+')[0].split('Z')[0].strip()
                                            try:
                                                parsed_dt = datetime.fromisoformat(value_no_tz)
                                                # Make timezone-aware (UTC)
                                                from datetime import timezone
                                                return parsed_dt.replace(tzinfo=timezone.utc)
                                            except ValueError:
                                                pass
                                        # Try parsing without timezone and add UTC
                                        try:
                                            from datetime import timezone
                                            parsed_dt = datetime.fromisoformat(value_clean)
                                            if parsed_dt.tzinfo is None:
                                                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                            return parsed_dt
                                        except ValueError:
                                            pass
                                else:
                                    # Try other common formats
                                    try:
                                        from dateutil import parser
                                        parsed_dt = parser.parse(value)
                                        # Ensure timezone-aware if needed
                                        if parsed_dt.tzinfo is None:
                                            from datetime import timezone
                                            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                        return parsed_dt
                                    except ImportError:
                                        # dateutil not available, try common formats
                                        from datetime import datetime as dt
                                        from datetime import timezone
                                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d %H:%M:%S.%f']:
                                            try:
                                                parsed_dt = dt.strptime(value, fmt)
                                                # Make timezone-aware (UTC)
                                                return parsed_dt.replace(tzinfo=timezone.utc)
                                            except ValueError:
                                                continue
                                        return value
                            except (ValueError, AttributeError, ImportError) as e:
                                try:
                                    # Fallback: try common formats
                                    from datetime import datetime as dt
                                    from datetime import timezone
                                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d %H:%M:%S.%f']:
                                        try:
                                            parsed_dt = dt.strptime(value, fmt)
                                            # Make timezone-aware (UTC)
                                            return parsed_dt.replace(tzinfo=timezone.utc)
                                        except ValueError:
                                            continue
                                    return value
                                except Exception:
                                    return value
                        elif is_type_match(date):
                            try:
                                return date.fromisoformat(value)
                            except (ValueError, AttributeError):
                                try:
                                    # Try parsing as datetime first, then extract date
                                    if 'T' in value or ' ' in value:
                                        try:
                                            dt_val = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                            return dt_val.date()
                                        except ValueError:
                                            # Try without timezone
                                            value_no_tz = value.split('+')[0].split('Z')[0].strip()
                                            dt_val = datetime.fromisoformat(value_no_tz)
                                            return dt_val.date()
                                    else:
                                        return date.fromisoformat(value)
                                except (ValueError, AttributeError):
                                    return value
                        # Also check if field name suggests datetime/date (e.g., created_at, updated_at, timestamp, etc.)
                        # Exclude timezone fields - they are strings, not datetime
                        elif ((field_name.endswith('_at') or field_name.endswith('_date') or field_name.endswith('_timestamp')) and 
                              'timezone' not in field_name.lower() and field_type):
                            origin = get_origin(field_type)
                            if origin is Union:
                                args = get_args(field_type)
                                if datetime in args:
                                    try:
                                        value_clean = value.replace('Z', '+00:00')
                                        try:
                                            return datetime.fromisoformat(value_clean)
                                        except ValueError:
                                            # Try without timezone
                                            value_no_tz = value_clean.split('+')[0].split('Z')[0].strip()
                                            return datetime.fromisoformat(value_no_tz)
                                    except (ValueError, AttributeError):
                                        try:
                                            from dateutil import parser
                                            parsed_dt = parser.parse(value)
                                            # Ensure timezone-aware if needed
                                            if parsed_dt.tzinfo is None:
                                                from datetime import timezone
                                                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                            return parsed_dt
                                        except (ImportError, ValueError, AttributeError):
                                            # Try common formats
                                            from datetime import datetime as dt
                                            from datetime import timezone
                                            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d %H:%M:%S.%f']:
                                                try:
                                                    parsed_dt = dt.strptime(value, fmt)
                                                    # Make timezone-aware (UTC)
                                                    return parsed_dt.replace(tzinfo=timezone.utc)
                                                except ValueError:
                                                    continue
                                            # If all parsing fails, log and return value to let Pydantic handle it
                                            logger.debug(f"Could not parse datetime string '{value}' for field '{field_name}'")
                                            return value
                                elif date in args:
                                    try:
                                        if 'T' in value or ' ' in value:
                                            try:
                                                dt_val = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                                return dt_val.date()
                                            except ValueError:
                                                value_no_tz = value.split('+')[0].split('Z')[0].strip()
                                                dt_val = datetime.fromisoformat(value_no_tz)
                                                return dt_val.date()
                                        else:
                                            return date.fromisoformat(value)
                                    except (ValueError, AttributeError):
                                        return value
                            elif field_type is datetime:
                                try:
                                    value_clean = value.replace('Z', '+00:00')
                                    try:
                                        return datetime.fromisoformat(value_clean)
                                    except ValueError:
                                        # Try without timezone
                                        value_no_tz = value_clean.split('+')[0].split('Z')[0].strip()
                                        return datetime.fromisoformat(value_no_tz)
                                except (ValueError, AttributeError):
                                    try:
                                        from dateutil import parser
                                        parsed_dt = parser.parse(value)
                                        # Ensure timezone-aware if needed
                                        if parsed_dt.tzinfo is None:
                                            from datetime import timezone
                                            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                        return parsed_dt
                                    except (ImportError, ValueError, AttributeError):
                                        # Try common formats
                                        from datetime import datetime as dt
                                        from datetime import timezone
                                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d %H:%M:%S.%f']:
                                            try:
                                                parsed_dt = dt.strptime(value, fmt)
                                                # Make timezone-aware (UTC)
                                                return parsed_dt.replace(tzinfo=timezone.utc)
                                            except ValueError:
                                                continue
                                        # If all parsing fails, log and return value to let Pydantic handle it
                                        logger.debug(f"Could not parse datetime string '{value}' for field '{field_name}'")
                                        return value
                            elif field_type is date:
                                try:
                                    if 'T' in value or ' ' in value:
                                        try:
                                            dt_val = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                            return dt_val.date()
                                        except ValueError:
                                            value_no_tz = value.split('+')[0].split('Z')[0].strip()
                                            dt_val = datetime.fromisoformat(value_no_tz)
                                            return dt_val.date()
                                    else:
                                        return date.fromisoformat(value)
                                except (ValueError, AttributeError):
                                    return value
                    
                    # For integers that might be strings or Decimals
                    if is_type_match(int):
                        if isinstance(value, str):
                            try:
                                return int(value)
                            except (ValueError, AttributeError):
                                return value
                        elif isinstance(value, Decimal):
                            return int(value)
                    
                    # For floats that might be strings or Decimals
                    if is_type_match(float):
                        if isinstance(value, str):
                            try:
                                return float(value)
                            except (ValueError, AttributeError):
                                return value
                        elif isinstance(value, Decimal):
                            return float(value)
                    
                    return value
                
                # Convert all values to Python types
                for field_name, field_info in model_fields.items():
                    if field_name in row_dict:
                        original_value = row_dict[field_name]
                        # If value is None and field has a default_factory, remove it from row_dict
                        # so Pydantic can use the default_factory
                        if original_value is None:
                            # Check if field has a default_factory (callable)
                            has_default_factory = (
                                hasattr(field_info, 'default_factory') and 
                                field_info.default_factory is not None and
                                callable(field_info.default_factory)
                            )
                            
                            if has_default_factory:
                                # Remove from row_dict so Pydantic uses default_factory
                                del row_dict[field_name]
                                logger.debug(f"Removed None value for field '{field_name}' with default_factory to let Pydantic use default")
                                continue
                            
                            # Check if field has a default value (not factory)
                            # A field has a default (not factory) if:
                            # - It has a default attribute that is not None
                            # - It does NOT have a callable default_factory
                            has_default = (
                                hasattr(field_info, 'default') and 
                                field_info.default is not None and
                                not (hasattr(field_info, 'default_factory') and 
                                     field_info.default_factory is not None and
                                     callable(field_info.default_factory))
                            )
                            
                            if has_default:
                                # Field has a default value, keep None (Pydantic will use default)
                                continue
                            
                            # If field is Optional, keep None
                            if hasattr(field_info, 'annotation'):
                                field_type = field_info.annotation
                                origin = get_origin(field_type)
                                if origin is Union:
                                    args = get_args(field_type)
                                    if type(None) in args:
                                        # It's Optional, keep None
                                        continue
                            
                            # Otherwise, skip None values (they'll be handled by Pydantic validation)
                            continue
                        
                        # Check if this is a datetime field BEFORE conversion
                        field_type = field_info.annotation
                        origin = get_origin(field_type)
                        is_datetime_type = False
                        if origin is Union:
                            args = get_args(field_type)
                            is_datetime_type = datetime in args
                        else:
                            is_datetime_type = field_type is datetime
                        
                        # Log datetime fields for debugging
                        # Exclude timezone fields - they are strings, not datetime
                        if (is_datetime_type or field_name.endswith('_at') or field_name.endswith('_timestamp')) and 'timezone' not in field_name.lower():
                            if is_datetime_type:
                                logger.debug(f"Converting datetime field '{field_name}': value={original_value}, type={type(original_value)}")
                        
                        converted_value = convert_spark_to_python(
                            original_value, 
                            field_name, 
                            field_info
                        )
                        
                        # Special handling for datetime fields - ensure they're properly converted
                        # Check both by type annotation AND by field name (for fields from base classes)
                        # Exclude timezone fields - they are strings, not datetime
                        is_datetime_field_by_name = (
                            (field_name.endswith('_at') or 
                             field_name.endswith('_timestamp') or 
                             field_name.endswith('_date')) and
                            'timezone' not in field_name.lower()
                        )
                        
                        if is_datetime_type or is_datetime_field_by_name:
                            # If the field expects datetime, ensure the converted value is a datetime
                            if not isinstance(converted_value, datetime):
                                # Try to convert one more time
                                if isinstance(converted_value, str):
                                    try:
                                        from dateutil import parser
                                        parsed_dt = parser.parse(converted_value)
                                        if parsed_dt.tzinfo is None:
                                            from datetime import timezone
                                            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                        converted_value = parsed_dt
                                        logger.debug(f"Converted datetime string '{converted_value}' for field '{field_name}'")
                                    except Exception as e:
                                        logger.warning(f"Could not parse datetime string for field '{field_name}': {converted_value}, error: {e}")
                                elif hasattr(converted_value, 'year') and hasattr(converted_value, 'month') and hasattr(converted_value, 'day'):
                                    # It's a datetime-like object but not a datetime - try to construct one
                                    try:
                                        hour = getattr(converted_value, 'hour', 0)
                                        minute = getattr(converted_value, 'minute', 0)
                                        second = getattr(converted_value, 'second', 0)
                                        microsecond = getattr(converted_value, 'microsecond', 0)
                                        converted_value = datetime(
                                            converted_value.year, converted_value.month, converted_value.day,
                                            hour, minute, second, microsecond
                                        )
                                        if converted_value.tzinfo is None:
                                            from datetime import timezone
                                            converted_value = converted_value.replace(tzinfo=timezone.utc)
                                        logger.debug(f"Converted datetime-like object to datetime for field '{field_name}'")
                                    except Exception as e:
                                        logger.warning(f"Could not convert datetime-like object for field '{field_name}': {e}")
                                else:
                                    # Try string conversion as last resort
                                    try:
                                        value_str = str(converted_value)
                                        from dateutil import parser
                                        parsed_dt = parser.parse(value_str)
                                        if parsed_dt.tzinfo is None:
                                            from datetime import timezone
                                            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                        converted_value = parsed_dt
                                        logger.debug(f"Converted via string parsing for field '{field_name}'")
                                    except Exception as e:
                                        logger.warning(f"Could not convert value to datetime for field '{field_name}': {converted_value} (type: {type(converted_value)}), error: {e}")
                            else:
                                # It's already a datetime - ensure it's timezone-aware
                                if converted_value.tzinfo is None:
                                    from datetime import timezone
                                    converted_value = converted_value.replace(tzinfo=timezone.utc)
                                    logger.debug(f"Made datetime timezone-aware for field '{field_name}'")
                        
                        # Also check by field name for datetime fields (catch fields from base classes)
                        # But skip timezone fields - they are strings, not datetime
                        if not is_datetime_type and is_datetime_field_by_name and 'timezone' not in field_name.lower():
                            # Field name suggests datetime but type annotation doesn't - try to convert anyway
                            if isinstance(converted_value, datetime):
                                # It's already a datetime - ensure it's timezone-aware
                                if converted_value.tzinfo is None:
                                    from datetime import timezone
                                    converted_value = converted_value.replace(tzinfo=timezone.utc)
                                    logger.debug(f"Made datetime field by name '{field_name}' timezone-aware: {converted_value}")
                            elif isinstance(converted_value, str):
                                try:
                                    from dateutil import parser
                                    parsed_dt = parser.parse(converted_value)
                                    if parsed_dt.tzinfo is None:
                                        from datetime import timezone
                                        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                    converted_value = parsed_dt
                                    logger.debug(f"Converted datetime field by name '{field_name}' from string: {converted_value}")
                                except Exception as e:
                                    logger.warning(f"Could not parse datetime string for field '{field_name}' (detected by name): {converted_value}, error: {e}")
                            elif hasattr(converted_value, 'year') and hasattr(converted_value, 'month') and hasattr(converted_value, 'day'):
                                # It's a datetime-like object but not a datetime - try to construct one
                                try:
                                    hour = getattr(converted_value, 'hour', 0)
                                    minute = getattr(converted_value, 'minute', 0)
                                    second = getattr(converted_value, 'second', 0)
                                    microsecond = getattr(converted_value, 'microsecond', 0)
                                    converted_value = datetime(
                                        converted_value.year, converted_value.month, converted_value.day,
                                        hour, minute, second, microsecond
                                    )
                                    if converted_value.tzinfo is None:
                                        from datetime import timezone
                                        converted_value = converted_value.replace(tzinfo=timezone.utc)
                                    logger.debug(f"Converted datetime field by name '{field_name}' from datetime-like object: {converted_value}")
                                except Exception as e:
                                    logger.warning(f"Could not convert datetime-like object for field '{field_name}' (detected by name): {e}")
                            elif converted_value is not None:
                                # Try string conversion as last resort
                                try:
                                    value_str = str(converted_value)
                                    from dateutil import parser
                                    parsed_dt = parser.parse(value_str)
                                    if parsed_dt.tzinfo is None:
                                        from datetime import timezone
                                        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                    converted_value = parsed_dt
                                    logger.debug(f"Converted datetime field by name '{field_name}' via string parsing: {converted_value}")
                                except Exception as e:
                                    logger.warning(f"Could not convert value to datetime for field '{field_name}' (detected by name): {converted_value} (type: {type(converted_value)}), error: {e}")
                        # If conversion didn't change the value and it's still a Decimal, try more aggressive conversion
                        if isinstance(converted_value, Decimal) and original_value == converted_value:
                            # Try to convert Decimal to int/float based on field name
                            if any(keyword in field_name.lower() for keyword in ['count', 'total', 'views', 'clicks', 'likes', 'shares', 'comments', 'bookmarks', 'plays', 'completions', 'conversions', 'signups', 'rank', 'id', 'revenue']):
                                # Likely an integer field
                                try:
                                    converted_value = int(converted_value)
                                except (ValueError, TypeError):
                                    pass
                            else:
                                # Likely a float field
                                try:
                                    converted_value = float(converted_value)
                                except (ValueError, TypeError):
                                    pass
                        
                        row_dict[field_name] = converted_value
                
                # Final pass: ensure all datetime fields are properly converted
                # This is a safety check to catch any datetime fields that weren't converted in the previous pass
                for field_name, field_info in model_fields.items():
                    if field_name in row_dict:
                        value = row_dict[field_name]
                        if value is not None:
                            field_type = field_info.annotation
                            origin = get_origin(field_type)
                            is_datetime_type = False
                            if origin is Union:
                                args = get_args(field_type)
                                is_datetime_type = datetime in args
                            else:
                                is_datetime_type = field_type is datetime
                            
                            # Also check by field name (for fields from base classes)
                            # Exclude timezone fields - they are strings, not datetime
                            is_datetime_field_by_name = (
                                (field_name.endswith('_at') or 
                                 field_name.endswith('_timestamp') or 
                                 field_name.endswith('_date')) and
                                'timezone' not in field_name.lower()
                            )
                            
                            # If field expects datetime (by type or name), ensure it's properly converted
                            if is_datetime_type or is_datetime_field_by_name:
                                # If value is already a datetime, ensure it's timezone-aware
                                if isinstance(value, datetime):
                                    if value.tzinfo is None:
                                        from datetime import timezone
                                        row_dict[field_name] = value.replace(tzinfo=timezone.utc)
                                        logger.debug(f"Final pass: Made datetime timezone-aware for field '{field_name}'")
                                    else:
                                        row_dict[field_name] = value
                                # If value is not a datetime, try to convert (this should rarely happen after the previous pass)
                                elif not isinstance(value, datetime):
                                    # Try one more time to convert
                                    if isinstance(value, str):
                                        try:
                                            from dateutil import parser
                                            parsed_dt = parser.parse(value)
                                            if parsed_dt.tzinfo is None:
                                                from datetime import timezone
                                                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                            row_dict[field_name] = parsed_dt
                                        except Exception as e:
                                            logger.warning(f"Final datetime conversion failed for '{field_name}': {value} (type: {type(value)}), error: {e}")
                                    elif hasattr(value, 'isoformat'):  # Might be a datetime-like object
                                        try:
                                            if isinstance(value, datetime):
                                                if value.tzinfo is None:
                                                    from datetime import timezone
                                                    row_dict[field_name] = value.replace(tzinfo=timezone.utc)
                                                else:
                                                    row_dict[field_name] = value
                                        except Exception:
                                            pass
                                    else:
                                        # Log what we got - this is the problem!
                                        logger.warning(f"Field '{field_name}' expects datetime but got {type(value)}: {value}. This will cause validation to fail.")
                                        # Try one last attempt - maybe it's a Spark TimestampType that needs special handling
                                        conversion_success = False
                                        try:
                                            # Try multiple conversion strategies
                                            # Strategy 1: Convert to string and parse
                                            value_str = str(value)
                                            from dateutil import parser
                                            parsed_dt = parser.parse(value_str)
                                            if parsed_dt.tzinfo is None:
                                                from datetime import timezone
                                                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                            row_dict[field_name] = parsed_dt
                                            conversion_success = True
                                            logger.debug(f"Successfully converted '{field_name}' from {type(value)} to datetime via string conversion")
                                        except Exception as e2:
                                            # Strategy 2: Try to extract datetime attributes if it's a datetime-like object
                                            try:
                                                if hasattr(value, 'year') and hasattr(value, 'month') and hasattr(value, 'day'):
                                                    hour = getattr(value, 'hour', 0)
                                                    minute = getattr(value, 'minute', 0)
                                                    second = getattr(value, 'second', 0)
                                                    microsecond = getattr(value, 'microsecond', 0)
                                                    parsed_dt = datetime(
                                                        value.year, value.month, value.day,
                                                        hour, minute, second, microsecond
                                                    )
                                                    if parsed_dt.tzinfo is None:
                                                        from datetime import timezone
                                                        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                                    row_dict[field_name] = parsed_dt
                                                    conversion_success = True
                                                    logger.debug(f"Successfully converted '{field_name}' from {type(value)} to datetime via attribute extraction")
                                            except Exception as e3:
                                                logger.warning(f"Could not convert '{field_name}' from {type(value)} to datetime. String parse error: {e2}, Attribute extraction error: {e3}")
                                        
                                        if not conversion_success:
                                            # Last resort: log the actual value and type for debugging
                                            logger.error(f"CRITICAL: Field '{field_name}' could not be converted to datetime. Value: {value}, Type: {type(value)}, Type name: {type(value).__name__}, Type module: {type(value).__module__}")
                
                try:
                    # Validate using Pydantic model
                    if strict:
                        model_instance = model_class.model_validate(row_dict, strict=True)
                    else:
                        model_instance = model_class.model_validate(row_dict)
                    
                    # Convert back to dict (with validated values)
                    validated_dict = model_instance.model_dump()
                    
                    # Convert types that Spark can't handle to Spark-compatible types
                    def convert_spark_types(obj):
                        """Recursively convert Spark-incompatible types to compatible ones."""
                        if obj is None:
                            return None
                        elif isinstance(obj, UUID):
                            return str(obj)
                        elif isinstance(obj, (date, datetime)):
                            return obj.isoformat()
                        elif isinstance(obj, dict):
                            return {k: convert_spark_types(v) for k, v in obj.items()}
                        elif isinstance(obj, list):
                            return [convert_spark_types(item) for item in obj]
                        elif isinstance(obj, (int, float, str, bool)):
                            # Already Spark-compatible
                            return obj
                        else:
                            # Convert unknown types to string
                            return str(obj) if obj is not None else None
                    
                    # Convert all Spark-incompatible types recursively
                    validated_dict = {k: convert_spark_types(v) for k, v in validated_dict.items()}
                    
                    return True, validated_dict, None
                    
                except ValidationError as e:
                    error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
                    error_str = "; ".join(error_messages)
                    return False, row_dict, error_str
                
                except Exception as e:
                    return False, row_dict, str(e)
            
            # Check if DataFrame is empty before validation
            row_count = df.count()
            logger.debug(f"DataFrame has {row_count} rows before validation")
            if row_count == 0:
                logger.warning("DataFrame is empty - no rows to validate")
                return df.limit(0), df.limit(0)
            
            # Apply validation to each row
            try:
                logger.debug(f"Starting validation for {row_count} rows")
                validation_results = df.rdd.map(validate_row).collect()
                logger.debug(f"Validation completed: {len(validation_results)} results returned")
            except Exception as e:
                logger.error(f"Error during validation mapping: {e}", exc_info=True)
                # Return empty DataFrames if validation fails
                return df.limit(0), df.limit(0)
            
            # Separate valid and invalid rows
            valid_rows = []
            invalid_rows = []
            
            if not validation_results:
                logger.warning(f"No validation results returned - validation_results is empty or None")
                return df.limit(0), df.limit(0)
            
            logger.debug(f"Processing {len(validation_results)} validation results")
            for idx, result in enumerate(validation_results):
                if not isinstance(result, tuple) or len(result) != 3:
                    logger.error(f"Invalid validation result at index {idx}: {result} (expected tuple of 3 elements)")
                    continue
                is_valid, row_dict, error = result
                if is_valid:
                    valid_rows.append(row_dict)
                else:
                    row_dict['_validation_error'] = error
                    invalid_rows.append(row_dict)
            
            logger.debug(f"Separated into {len(valid_rows)} valid rows and {len(invalid_rows)} invalid rows")
            
            # Create DataFrames
            if valid_rows:
                try:
                    # Try to create DataFrame with schema inference
                    valid_df = df.sparkSession.createDataFrame(valid_rows)
                except (ValueError, TypeError) as e:
                    # If schema inference fails, try using the original DataFrame's schema
                    logger.warning(
                        f"Could not infer schema for valid rows: {e}. "
                        f"Trying to use original DataFrame schema."
                    )
                    try:
                        # Use original schema but convert rows to match
                        # Create DataFrame with explicit schema from original
                        original_schema = df.schema
                        # Convert rows to Row objects matching the schema
                        from pyspark.sql import Row as SparkRow
                        from pyspark.sql.types import StringType
                        
                        schema_field_names = [f.name for f in original_schema.fields]
                        valid_rows_typed = []
                        for row_dict in valid_rows:
                            # Create Row with only fields that exist in schema
                            # Convert all values to match schema types
                            row_data = {}
                            for field in original_schema.fields:
                                field_name = field.name
                                value = row_dict.get(field_name)
                                # Convert value to match field type if needed
                                if value is not None:
                                    # If field is StringType or if value is not matching, convert to string
                                    if isinstance(field.dataType, StringType) or not isinstance(value, (str, int, float, bool, type(None))):
                                        row_data[field_name] = str(value) if value is not None else None
                                    else:
                                        row_data[field_name] = value
                                else:
                                    row_data[field_name] = None
                            valid_rows_typed.append(SparkRow(**row_data))
                        
                        # Try with original schema first
                        try:
                            valid_df = df.sparkSession.createDataFrame(valid_rows_typed, original_schema)
                        except Exception:
                            # If that fails, create a permissive schema (all strings)
                            from pyspark.sql.types import StructType, StructField
                            permissive_schema = StructType([
                                StructField(f.name, StringType(), True) for f in original_schema.fields
                            ])
                            valid_df = df.sparkSession.createDataFrame(valid_rows_typed, permissive_schema)
                    except Exception as e2:
                        logger.error(
                            f"Could not create DataFrame with original schema: {e2}. "
                            f"Returning empty DataFrame."
                        )
                        valid_df = df.limit(0)
            else:
                # Return empty DataFrame with same schema
                valid_df = df.limit(0)
            
            if invalid_rows:
                try:
                    invalid_df = df.sparkSession.createDataFrame(invalid_rows)
                except (ValueError, Exception) as e:
                    # If we can't create DataFrame from invalid rows (e.g., type inference issues),
                    # just return an empty DataFrame with the same schema
                    logger.warning(
                        f"Could not create DataFrame from invalid rows: {e}. "
                        f"Returning empty DataFrame instead."
                    )
                    invalid_df = df.limit(0)
            else:
                invalid_df = df.limit(0)
            
            logger.info(
                f"Validation complete: {len(valid_rows)} valid rows, "
                f"{len(invalid_rows)} invalid rows"
            )
            
            if invalid_rows:
                logger.warning(f"Validation errors: {[r.get('_validation_error') for r in invalid_rows[:5]]}")
            
            return valid_df, invalid_df
            
        except Exception as e:
            logger.error(f"Error validating DataFrame: {e}", exc_info=True)
            # Return original DataFrame as invalid if validation fails
            return df.limit(0), df
    
    @staticmethod
    def validate_and_filter(
        df: DataFrame,
        model_class: Type[BaseModel],
        strict: bool = False
    ) -> DataFrame:
        """
        Validate DataFrame and return only valid rows.
        
        Args:
            df: Spark DataFrame to validate
            model_class: Pydantic model class to validate against
            strict: Whether to use strict validation
            
        Returns:
            DataFrame with only valid rows
        """
        valid_df, _ = ModelValidator.validate_dataframe(df, model_class, strict, filter_invalid=True)
        return valid_df
    
    @staticmethod
    def add_validation_column(
        df: DataFrame,
        model_class: Type[BaseModel],
        validation_column: str = "is_valid"
    ) -> DataFrame:
        """
        Add validation column to DataFrame indicating if row is valid.
        
        Args:
            df: Spark DataFrame to validate
            model_class: Pydantic model class to validate against
            validation_column: Name of validation column
            
        Returns:
            DataFrame with validation column added
        """
        def is_valid_row(row: Row) -> bool:
            """Check if row is valid."""
            try:
                row_dict = row.asDict()
                model_class.model_validate(row_dict)
                return True
            except Exception:
                return False
        
        # Create UDF for validation
        is_valid_udf = udf(is_valid_row, BooleanType())
        
        # Add validation column
        return df.withColumn(validation_column, is_valid_udf(col("*")))
    
    @staticmethod
    def transform_to_model_schema(
        df: DataFrame,
        model_class: Type[BaseModel]
    ) -> DataFrame:
        """
        Transform DataFrame to match Pydantic model schema.
        
        This ensures:
        - Column names match model fields
        - Column types match model types
        - Required fields are present
        - Optional fields are handled
        
        Args:
            df: Spark DataFrame to transform
            model_class: Pydantic model class
            
        Returns:
            Transformed DataFrame matching model schema
        """
        try:
            # Check DataFrame before transformation
            row_count_before = df.count()
            logger.debug(f"DataFrame before transformation: {row_count_before} rows, {len(df.columns)} columns")
            logger.debug(f"DataFrame columns: {df.columns}")
            
            if row_count_before == 0:
                logger.warning("DataFrame is already empty before transformation")
                return df
            
            # Get model fields
            model_fields = model_class.model_fields
            logger.debug(f"Model {model_class.__name__} has {len(model_fields)} fields")
            
            # Select and rename columns to match model
            select_exprs = []
            missing_columns = []
            for field_name, field_info in model_fields.items():
                # Check if column exists in DataFrame
                if field_name in df.columns:
                    select_exprs.append(col(field_name).alias(field_name))
                else:
                    # Field not in DataFrame - use None for optional, or raise error for required
                    missing_columns.append(field_name)
                    if field_info.is_required():
                        logger.warning(f"Required field {field_name} not found in DataFrame, using null")
                        select_exprs.append(lit(None).alias(field_name))
                    else:
                        # Optional field, use None
                        select_exprs.append(lit(None).alias(field_name))
            
            if missing_columns:
                logger.debug(f"Missing columns in DataFrame: {missing_columns}")
            
            if not select_exprs:
                logger.error("No select expressions generated - this should not happen!")
                return df.limit(0)
            
            logger.debug(f"Generated {len(select_exprs)} select expressions")
            
            # Select columns matching model schema
            transformed_df = df.select(*select_exprs)
            
            # Check DataFrame after transformation
            row_count_after = transformed_df.count()
            logger.debug(f"DataFrame after transformation: {row_count_after} rows, {len(transformed_df.columns)} columns")
            
            if row_count_after == 0 and row_count_before > 0:
                logger.error(f"DataFrame became empty after transformation! Before: {row_count_before} rows, After: {row_count_after} rows")
                logger.error(f"This should not happen - select() should not filter rows")
            
            logger.info(f"Transformed DataFrame to match {model_class.__name__} schema")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error transforming DataFrame to model schema: {e}", exc_info=True)
            return df

