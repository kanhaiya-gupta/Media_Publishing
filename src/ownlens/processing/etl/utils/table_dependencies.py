"""
Table Dependencies Utility
==========================

Defines table dependency order and dependency resolution for ETL loading.
This ensures tables are loaded in the correct order to respect foreign key constraints.

Critical for PostgreSQL loading to avoid foreign key violations.
"""

from typing import List, Dict, Set


# Tables in dependency order (seed tables first, then dependent tables)
# This ensures foreign key references exist when loading dependent tables
TABLE_DEPENDENCY_ORDER = [
    # Level 1: Base/seed tables (no dependencies)
    "countries",
    "cities",
    "device_types",
    "operating_systems",
    "browsers",
    
    # Level 2: Companies and brands (depend on countries)
    "companies",
    "brands",
    "brand_countries",
    
    # Level 3: Categories (depends on brands)
    "categories",
    
    # Level 4: Users (depends on countries)
    "users",
    
    # Level 5: User accounts (depends on users, companies, brands, countries)
    "user_accounts",
    
    # Level 6: Security (depends on users, companies, brands)
    "security_permissions",
    "security_roles",
    "security_role_permissions",
    "security_user_roles",
    "security_api_keys",
    "security_api_key_usage",
    "security_user_sessions",
    
    # Level 7: Customer domain (depends on users, companies, brands, device_types, os, browsers, cities, countries)
    "customer_sessions",
    "customer_events",
    "customer_user_segments",
    "customer_user_segment_assignments",
    "customer_user_features",
    "customer_churn_predictions",
    "customer_recommendations",
    "customer_conversion_predictions",
    
    # Level 8: Company domain (depends on companies, brands, countries, cities, users, categories)
    "company_departments",
    "company_employees",
    "company_employee_engagement",
    "company_internal_content",
    "company_content_performance",
    "company_content_events",
    "company_department_performance",
    "company_communications_analytics",
    
    # Level 9: Editorial core (depends on brands, users, categories)
    "editorial_authors",
    "editorial_articles",
    "editorial_article_performance",
    "editorial_author_performance",
    "editorial_category_performance",
    "editorial_content_events",
    "editorial_headline_tests",
    "editorial_trending_topics",
    "editorial_content_recommendations",
    "editorial_article_content",
    "editorial_content_versions",
    
    # Level 10: Editorial media (depends on companies, brands)
    "editorial_media_assets",
    "editorial_media_variants",
    "editorial_media_collections",
    "editorial_media_collection_items",
    "editorial_content_media",
    "editorial_media_usage",
    
    # Level 11: Compliance (depends on companies, brands, users)
    "compliance_user_consent",
    "compliance_data_subject_requests",
    "compliance_retention_policies",
    "compliance_retention_executions",
    "compliance_anonymized_data",
    "compliance_privacy_assessments",
    "compliance_breach_incidents",
    
    # Level 12: Audit (depends on users, companies, brands)
    "audit_logs",
    "audit_data_changes",
    "audit_data_access",
    "audit_data_lineage",
    "audit_security_events",
    "audit_compliance_events",
    
    # Level 13: Data quality (depends on companies, brands)
    "data_quality_rules",
    "data_quality_checks",
    "data_quality_metrics",
    "data_quality_alerts",
    "data_validation_results",
    
    # Level 14: ML models (depends on companies, brands, users)
    "ml_model_registry",
    "ml_model_features",
    "ml_model_training_runs",
    "ml_model_predictions",
    "ml_model_monitoring",
    "ml_model_ab_tests",
    
    # Level 15: Configuration (depends on companies, brands, users)
    "configuration_feature_flags",
    "configuration_feature_flag_history",
    "configuration_system_settings",
    "configuration_system_settings_history",
]


# Table dependencies mapping (for automatic dependency resolution)
# Maps each table to its required dependencies
TABLE_DEPENDENCIES: Dict[str, List[str]] = {
    # Base tables have no dependencies
    "countries": [],
    "cities": [],
    "device_types": [],
    "operating_systems": [],
    "browsers": [],
    
    # Level 2
    "companies": ["countries"],
    "brands": ["companies", "countries"],
    "brand_countries": ["brands", "countries"],
    
    # Level 3
    "categories": ["brands"],
    
    # Level 4
    "users": ["countries"],
    
    # Level 5
    "user_accounts": ["users", "companies", "brands", "countries"],
    
    # Level 6: Security
    "security_permissions": [],
    "security_roles": [],
    "security_role_permissions": ["security_roles", "security_permissions"],
    "security_user_roles": ["users", "security_roles", "companies", "brands"],
    "security_api_keys": ["users", "companies", "brands"],
    "security_api_key_usage": ["security_api_keys"],
    "security_user_sessions": ["users"],
    
    # Level 7: Customer domain
    "customer_sessions": ["users", "companies", "brands", "countries", "cities", "device_types", "operating_systems", "browsers"],
    "customer_events": ["customer_sessions", "users", "companies", "brands", "categories", "countries", "cities", "device_types", "operating_systems", "browsers"],
    "customer_user_segments": ["companies", "brands"],
    "customer_user_segment_assignments": ["users", "companies", "brands", "customer_user_segments"],
    "customer_user_features": ["users", "companies", "brands", "countries", "cities", "device_types", "operating_systems", "browsers", "categories"],
    "customer_churn_predictions": ["users", "companies", "brands"],
    "customer_recommendations": ["users", "companies", "brands", "categories"],
    "customer_conversion_predictions": ["users", "companies", "brands"],
    
    # Level 8: Company domain
    "company_departments": ["companies", "brands", "countries", "cities"],
    "company_employees": ["users", "companies", "brands", "company_departments"],
    "company_employee_engagement": ["company_employees", "companies", "brands"],
    "company_internal_content": ["companies", "brands", "company_departments", "company_employees", "categories"],
    "company_content_performance": ["company_internal_content", "companies", "brands"],
    "company_content_events": ["company_internal_content", "users", "companies", "brands"],
    "company_department_performance": ["company_departments", "companies", "brands"],
    "company_communications_analytics": ["companies", "brands", "company_departments"],
    
    # Level 9: Editorial core
    "editorial_authors": ["users", "brands"],
    "editorial_articles": ["editorial_authors", "brands", "categories"],
    "editorial_article_performance": ["editorial_articles", "brands"],
    "editorial_author_performance": ["editorial_authors", "brands"],
    "editorial_category_performance": ["categories", "brands"],
    "editorial_content_events": ["editorial_articles", "users", "brands"],
    "editorial_headline_tests": ["editorial_articles", "brands"],
    "editorial_trending_topics": ["brands"],
    "editorial_content_recommendations": ["editorial_articles", "users", "brands"],
    "editorial_article_content": ["editorial_articles"],
    "editorial_content_versions": ["editorial_articles", "editorial_article_content"],  # content_id FK requires editorial_article_content
    
    # Level 10: Editorial media
    "editorial_media_assets": ["companies", "brands"],
    "editorial_media_variants": ["editorial_media_assets"],
    "editorial_media_collections": ["companies", "brands"],
    "editorial_media_collection_items": ["editorial_media_collections", "editorial_media_assets"],
    "editorial_content_media": ["editorial_articles", "editorial_media_assets"],
    "editorial_media_usage": ["editorial_media_assets", "brands"],
    
    # Level 11: Compliance
    "compliance_user_consent": ["users", "companies", "brands"],
    "compliance_data_subject_requests": ["users", "companies", "brands"],
    "compliance_retention_policies": ["companies", "brands"],
    "compliance_retention_executions": ["compliance_retention_policies", "companies", "brands"],
    "compliance_anonymized_data": ["companies", "brands", "users"],
    "compliance_privacy_assessments": ["companies", "brands"],
    "compliance_breach_incidents": ["companies", "brands"],
    
    # Level 12: Audit
    "audit_logs": ["users", "companies", "brands"],
    "audit_data_changes": ["users", "companies", "brands"],
    "audit_data_access": ["users", "companies", "brands"],
    "audit_data_lineage": ["companies", "brands"],
    "audit_security_events": ["users", "companies", "brands"],
    "audit_compliance_events": ["companies", "brands"],
    
    # Level 13: Data quality
    "data_quality_rules": ["companies", "brands"],
    "data_quality_checks": ["data_quality_rules", "companies", "brands"],
    "data_quality_metrics": ["companies", "brands"],
    "data_quality_alerts": ["data_quality_checks", "companies", "brands"],
    "data_validation_results": ["companies", "brands"],
    
    # Level 14: ML models
    "ml_model_registry": ["companies", "brands"],
    "ml_model_features": ["ml_model_registry"],
    "ml_model_training_runs": ["ml_model_registry", "companies", "brands"],
    "ml_model_predictions": ["ml_model_registry", "users", "companies", "brands"],
    "ml_model_monitoring": ["ml_model_registry", "companies", "brands"],
    "ml_model_ab_tests": ["ml_model_registry", "companies", "brands"],
    
    # Level 15: Configuration
    "configuration_feature_flags": ["companies", "brands"],
    "configuration_feature_flag_history": ["configuration_feature_flags", "users"],
    "configuration_system_settings": ["companies", "brands", "users"],
    "configuration_system_settings_history": ["configuration_system_settings", "users"],
}


def resolve_dependencies(tables: List[str]) -> List[str]:
    """
    Resolve all dependencies for the given tables.
    Returns tables in dependency order (dependencies first).
    
    Args:
        tables: List of table names to resolve
        
    Returns:
        List of tables in dependency order, including all required dependencies
        
    Example:
        >>> resolve_dependencies(["customer_events"])
        ["countries", "cities", "device_types", "operating_systems", "browsers",
         "companies", "brands", "categories", "users", "customer_sessions", "customer_events"]
    """
    resolved: Set[str] = set()
    to_resolve = set(tables)
    
    def add_with_deps(table: str) -> None:
        """Recursively add table and its dependencies."""
        if table in resolved:
            return
        
        # Add dependencies first
        deps = TABLE_DEPENDENCIES.get(table, [])
        for dep in deps:
            if dep in TABLE_DEPENDENCY_ORDER:  # Only add if it's a known table
                add_with_deps(dep)
        
        # Then add the table itself
        resolved.add(table)
    
    # Resolve all requested tables
    for table in to_resolve:
        if table in TABLE_DEPENDENCY_ORDER:
            add_with_deps(table)
    
    # Return in dependency order
    return [t for t in TABLE_DEPENDENCY_ORDER if t in resolved]


def sort_tables_by_dependency(tables: List[str]) -> List[str]:
    """
    Sort a list of tables by their dependency order.
    Tables not in TABLE_DEPENDENCY_ORDER are placed at the end.
    
    Args:
        tables: List of table names to sort
        
    Returns:
        List of tables sorted by dependency order
    """
    # Separate known and unknown tables
    known_tables = [t for t in TABLE_DEPENDENCY_ORDER if t in tables]
    unknown_tables = [t for t in tables if t not in TABLE_DEPENDENCY_ORDER]
    
    return known_tables + unknown_tables

