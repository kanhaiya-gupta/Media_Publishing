"""
OwnLens - ML Module: Schema Mapper

Utility to map between old schema (session_metrics) and new schema (customer_sessions).
"""

from typing import Dict, Optional


# Mapping from old field names to new field names
OLD_TO_NEW_FIELD_MAP: Dict[str, str] = {
    # Table name
    'session_metrics': 'customer_sessions',
    
    # Geographic fields
    'country': 'country_code',
    'city': 'city_id',
    
    # Device fields
    'device_type': 'device_type_id',
    'device_os': 'os_id',
    'browser': 'browser_id',
    
    # Brand/Company fields (if brand was a string, now it's brand_id)
    'brand': 'brand_id',
    
    # Note: subscription_tier and user_segment remain the same
    # Note: Most metric fields remain the same (session_duration_sec, etc.)
}

# Reverse mapping
NEW_TO_OLD_FIELD_MAP: Dict[str, str] = {v: k for k, v in OLD_TO_NEW_FIELD_MAP.items()}


def get_table_name(use_new_schema: bool = True) -> str:
    """
    Get the correct table name based on schema version.
    
    Args:
        use_new_schema: If True, return new table name, else old
    
    Returns:
        Table name string
    """
    return 'customer_sessions' if use_new_schema else 'session_metrics'


def map_field_name(field_name: str, use_new_schema: bool = True) -> str:
    """
    Map a field name between old and new schema.
    
    Args:
        field_name: Field name to map
        use_new_schema: If True, map to new schema, else to old schema
    
    Returns:
        Mapped field name
    """
    if use_new_schema:
        # Map old -> new
        return OLD_TO_NEW_FIELD_MAP.get(field_name, field_name)
    else:
        # Map new -> old
        return NEW_TO_OLD_FIELD_MAP.get(field_name, field_name)


def get_field_mapping(use_new_schema: bool = True) -> Dict[str, str]:
    """
    Get the complete field mapping dictionary.
    
    Args:
        use_new_schema: If True, return old->new mapping, else new->old
    
    Returns:
        Field mapping dictionary
    """
    if use_new_schema:
        return OLD_TO_NEW_FIELD_MAP.copy()
    else:
        return NEW_TO_OLD_FIELD_MAP.copy()





