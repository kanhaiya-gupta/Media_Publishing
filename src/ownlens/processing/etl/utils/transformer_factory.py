"""
Transformer Factory
==================

Automatically selects the appropriate transformer based on table/topic name.
"""

from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
import logging

from ..transformers.base_transformer import BaseDataTransformer
from ..transformers.base.base import (
    CompanyTransformer,
    BrandTransformer,
    BrandCountryTransformer,
    CountryTransformer,
    CityTransformer,
    CategoryTransformer,
    UserTransformer,
    UserAccountTransformer,
    DeviceTypeTransformer,
    OperatingSystemTransformer,
    BrowserTransformer,
)
from ..transformers.customer.customer import (
    UserEventTransformer,
    SessionTransformer,
    FeatureEngineeringTransformer,
    UserSegmentTransformer,
    UserSegmentAssignmentTransformer,
    ChurnPredictionTransformer,
    RecommendationTransformer,
    ConversionPredictionTransformer,
)
from ..transformers.editorial.editorial import (
    ArticleTransformer,
    PerformanceTransformer,
    AuthorPerformanceTransformer,
    CategoryPerformanceTransformer,
    ArticleContentTransformer,
    AuthorTransformer,
    HeadlineTestTransformer,
    TrendingTopicTransformer,
    ContentRecommendationTransformer,
    ContentVersionTransformer,
    MediaAssetTransformer,
    MediaVariantTransformer,
    ContentMediaTransformer,
    MediaCollectionTransformer,
    MediaCollectionItemTransformer,
    MediaUsageTransformer,
    EditorialContentEventTransformer,
)
from ..transformers.company.company import (
    EngagementTransformer,
    DepartmentTransformer,
    EmployeeTransformer,
    CommunicationsAnalyticsTransformer,
    ContentPerformanceTransformer,
    DepartmentPerformanceTransformer,
    ContentEventTransformer,
    InternalContentTransformer,
)
from ..transformers.audit.audit import (
    AuditLogTransformer,
    DataChangeTransformer,
    DataAccessTransformer,
    DataLineageTransformer,
    SecurityEventTransformer as AuditSecurityEventTransformer,
    ComplianceEventTransformer as AuditComplianceEventTransformer,
)
from ..transformers.security.security import (
    SecurityEventTransformer,
    RoleTransformer,
    PermissionTransformer,
    RolePermissionTransformer,
    UserRoleTransformer,
    APIKeyTransformer,
    APIKeyUsageTransformer,
    UserSessionTransformer,
)
from ..transformers.compliance.compliance import (
    ComplianceEventTransformer,
    UserConsentTransformer,
    DataSubjectRequestTransformer,
    RetentionPolicyTransformer,
    RetentionExecutionTransformer,
    AnonymizedDataTransformer,
    PrivacyAssessmentTransformer,
    BreachIncidentTransformer,
)
from ..transformers.ml_models.ml_models import (
    ModelPredictionTransformer,
    ModelRegistryTransformer,
    ModelFeatureTransformer,
    TrainingRunTransformer,
    ModelABTestTransformer,
    ModelMonitoringTransformer,
)
from ..transformers.data_quality.data_quality import (
    QualityMetricTransformer,
    QualityRuleTransformer,
    QualityCheckTransformer,
    QualityAlertTransformer,
    ValidationResultTransformer,
)
from ..transformers.configuration.configuration import (
    FeatureFlagTransformer,
    FeatureFlagHistoryTransformer,
    SystemSettingTransformer,
    SystemSettingHistoryTransformer,
)

logger = logging.getLogger(__name__)


class TransformerFactory:
    """
    Factory for automatically selecting transformers based on table/topic names.
    
    Maps table/topic names to their appropriate domain-specific transformers
    that include Pydantic model validation.
    """
    
    # Mapping of table/topic names to transformer classes
    TRANSFORMER_MAP: Dict[str, type] = {
        # Base domain
        "companies": CompanyTransformer,
        "brands": BrandTransformer,
        "brand_countries": BrandCountryTransformer,
        "countries": CountryTransformer,
        "cities": CityTransformer,
        "categories": CategoryTransformer,
        "users": UserTransformer,
        "user_accounts": UserAccountTransformer,
        "device_types": DeviceTypeTransformer,
        "operating_systems": OperatingSystemTransformer,
        "browsers": BrowserTransformer,
        
        # Customer domain
        "customer_events": UserEventTransformer,
        "customer_user_events": UserEventTransformer,
        "customer-user-events": UserEventTransformer,
        "customer_sessions": SessionTransformer,
        "customer-sessions": SessionTransformer,
        "customer_user_features": FeatureEngineeringTransformer,
        "customer_user_segments": UserSegmentTransformer,
        "customer_user_segment_assignments": UserSegmentAssignmentTransformer,
        "customer_churn_predictions": ChurnPredictionTransformer,
        "customer_recommendations": RecommendationTransformer,
        "customer_conversion_predictions": ConversionPredictionTransformer,
        
        # Editorial domain
        "editorial_articles": ArticleTransformer,
        "editorial_article_performance": PerformanceTransformer,
        "editorial_author_performance": AuthorPerformanceTransformer,
        "editorial_category_performance": CategoryPerformanceTransformer,
        "editorial_content_events": EditorialContentEventTransformer,
        "editorial-content-events": EditorialContentEventTransformer,
        "editorial_article_content": ArticleContentTransformer,
        "editorial_authors": AuthorTransformer,
        "editorial_headline_tests": HeadlineTestTransformer,
        "editorial_trending_topics": TrendingTopicTransformer,
        "editorial_content_recommendations": ContentRecommendationTransformer,
        "editorial_content_versions": ContentVersionTransformer,
        "editorial_media_assets": MediaAssetTransformer,
        "editorial_media_variants": MediaVariantTransformer,
        "editorial_content_media": ContentMediaTransformer,
        "editorial_media_collections": MediaCollectionTransformer,
        "editorial_media_collection_items": MediaCollectionItemTransformer,
        "editorial_media_usage": MediaUsageTransformer,
        
        # Company domain
        "company_employees": EmployeeTransformer,
        "company_employee_engagement": EngagementTransformer,
        "company_internal_content": InternalContentTransformer,
        "company_content_events": ContentEventTransformer,
        "company_content_performance": ContentPerformanceTransformer,
        "company_department_performance": DepartmentPerformanceTransformer,
        "company_departments": DepartmentTransformer,
        "company_communications_analytics": CommunicationsAnalyticsTransformer,
        
        # Security domain
        "security_events": SecurityEventTransformer,
        "security-events": SecurityEventTransformer,
        "security_roles": RoleTransformer,
        "security_permissions": PermissionTransformer,
        "security_role_permissions": RolePermissionTransformer,
        "security_user_roles": UserRoleTransformer,
        "security_api_keys": APIKeyTransformer,
        "security_api_key_usage": APIKeyUsageTransformer,
        "security_user_sessions": UserSessionTransformer,
        
        # Audit domain
        "audit_logs": AuditLogTransformer,
        "audit_data_changes": DataChangeTransformer,
        "audit_data_access": DataAccessTransformer,
        "audit_security_events": AuditSecurityEventTransformer,
        "audit_data_lineage": DataLineageTransformer,
        "audit_compliance_events": AuditComplianceEventTransformer,
        
        # Compliance domain
        "compliance_events": ComplianceEventTransformer,
        "compliance-events": ComplianceEventTransformer,
        "compliance_user_consent": UserConsentTransformer,
        "compliance_data_subject_requests": DataSubjectRequestTransformer,
        "compliance_retention_policies": RetentionPolicyTransformer,
        "compliance_retention_executions": RetentionExecutionTransformer,
        "compliance_anonymized_data": AnonymizedDataTransformer,
        "compliance_privacy_assessments": PrivacyAssessmentTransformer,
        "compliance_breach_incidents": BreachIncidentTransformer,
        
        # ML Models domain
        "ml_model_registry": ModelRegistryTransformer,
        "ml_model_features": ModelFeatureTransformer,
        "ml_model_training_runs": TrainingRunTransformer,
        "ml_model_predictions": ModelPredictionTransformer,
        "ml_model_monitoring": ModelMonitoringTransformer,
        "ml_model_ab_tests": ModelABTestTransformer,
        
        # Data Quality domain
        "data_quality_rules": QualityRuleTransformer,
        "data_quality_checks": QualityCheckTransformer,
        "data_quality_metrics": QualityMetricTransformer,
        "data_quality_alerts": QualityAlertTransformer,
        "data_validation_results": ValidationResultTransformer,
        
        # Configuration domain
        "configuration_feature_flags": FeatureFlagTransformer,
        "configuration_feature_flag_history": FeatureFlagHistoryTransformer,
        "configuration_system_settings": SystemSettingTransformer,
        "configuration_system_settings_history": SystemSettingHistoryTransformer,
    }
    
    @classmethod
    def get_transformer(
        cls,
        spark: SparkSession,
        table_or_topic_name: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Optional[BaseDataTransformer]:
        """
        Get the appropriate transformer for a table or topic.
        
        Args:
            spark: SparkSession instance
            table_or_topic_name: Name of table or topic
            config: Optional transformer configuration
            
        Returns:
            Transformer instance (domain-specific if available, otherwise BaseDataTransformer, or None for S3 paths)
        """
        # Normalize name (lowercase, handle underscores/hyphens)
        normalized_name = table_or_topic_name.lower().strip()
        
        # S3 paths contain file metadata (file_path, file_name, file_size) and don't need business transformers
        # Common S3 path patterns that should not be transformed
        s3_path_patterns = ["", "images/", "videos/", "documents/", "audio/", "backups/"]
        if normalized_name in [p.rstrip("/").lower() for p in s3_path_patterns]:
            logger.debug(f"S3 path '{table_or_topic_name}' contains file metadata - no transformer needed")
            return None
        
        # Try exact match first
        transformer_class = cls.TRANSFORMER_MAP.get(normalized_name)
        
        # If found BaseDataTransformer, treat as None to try partial matching
        if transformer_class == BaseDataTransformer:
            transformer_class = None
        
        # If no exact match, try partial matching (skip BaseDataTransformer entries)
        # But skip if normalized_name is empty (S3 root path)
        if transformer_class is None and normalized_name:
            for key, transformer_cls in cls.TRANSFORMER_MAP.items():
                if transformer_cls != BaseDataTransformer and (key in normalized_name or normalized_name in key):
                    transformer_class = transformer_cls
                    logger.info(f"Matched '{table_or_topic_name}' to transformer '{key}'")
                    break
        
        # If no specific transformer found, use BaseDataTransformer
        if transformer_class is None:
            transformer_class = BaseDataTransformer
            logger.debug(
                f"No specific transformer found for '{table_or_topic_name}', "
                f"using BaseDataTransformer"
            )
        else:
            logger.info(
                f"Using {transformer_class.__name__} for '{table_or_topic_name}'"
            )
        
        # Create transformer instance with config
        # Create a copy to avoid mutating the original config
        transformer_config = (config or {}).copy()
        # Enable validation by default for domain-specific transformers
        if transformer_class != BaseDataTransformer:
            transformer_config.setdefault("validate_with_model", True)
        
        return transformer_class(spark, transformer_config)
    
    @classmethod
    def register_transformer(
        cls,
        table_or_topic_name: str,
        transformer_class: type
    ) -> None:
        """
        Register a new transformer mapping.
        
        Args:
            table_or_topic_name: Name of table or topic
            transformer_class: Transformer class to use
        """
        normalized_name = table_or_topic_name.lower().strip()
        cls.TRANSFORMER_MAP[normalized_name] = transformer_class
        logger.info(
            f"Registered transformer '{transformer_class.__name__}' "
            f"for '{normalized_name}'"
        )

