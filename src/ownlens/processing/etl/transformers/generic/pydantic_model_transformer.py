"""
Generic Pydantic Model Transformer
==================================

Automatically validates data against Pydantic models based on table name.
This transformer can handle any table that has a corresponding Pydantic model.
"""

from typing import Optional, Dict, Any, Type
from pyspark.sql import DataFrame, SparkSession
from pydantic import BaseModel

from ..base_transformer import BaseDataTransformer
from ...utils.model_validator import ModelValidator

import logging

logger = logging.getLogger(__name__)


class PydanticModelTransformer(BaseDataTransformer):
    """
    Generic transformer that validates data against Pydantic models.
    
    Automatically finds the corresponding Pydantic model based on table name
    and validates the data against it.
    """
    
    # Mapping of table names to Pydantic model modules
    MODEL_MODULE_MAP = {
        # Base domain
        "companies": ("base.company", "Company", "CompanyCreate"),
        "brands": ("base.brand", "Brand", "BrandCreate"),
        "brand_countries": ("base.brand_country", "BrandCountry", "BrandCountryCreate"),
        "countries": ("base.country", "Country", "CountryCreate"),
        "cities": ("base.city", "City", "CityCreate"),
        "categories": ("base.category", "Category", "CategoryCreate"),
        "users": ("customer.user", "User", "UserCreate"),
        "user_accounts": ("base.user_account", "UserAccount", "UserAccountCreate"),
        "device_types": ("base.device_type", "DeviceType", "DeviceTypeCreate"),
        "operating_systems": ("base.operating_system", "OperatingSystem", "OperatingSystemCreate"),
        "browsers": ("base.browser", "Browser", "BrowserCreate"),
        
        # Customer domain
        "customer_sessions": ("customer.session", "Session", "SessionCreate"),
        "customer_events": ("customer.user_event", "UserEvent", "UserEventCreate"),
        "customer_user_features": ("customer.user_features", "UserFeatures", "UserFeaturesCreate"),
        "customer_user_segments": ("customer.user_segment", "UserSegment", "UserSegmentCreate"),
        "customer_churn_predictions": ("customer.churn_prediction", "ChurnPrediction", "ChurnPredictionCreate"),
        "customer_recommendations": ("customer.recommendation", "Recommendation", "RecommendationCreate"),
        "customer_conversion_predictions": ("customer.conversion_prediction", "ConversionPrediction", "ConversionPredictionCreate"),
        
        # Editorial domain
        "editorial_authors": ("editorial.author", "Author", "AuthorCreate"),
        "editorial_articles": ("editorial.article", "Article", "ArticleCreate"),
        "editorial_article_performance": ("editorial.article_performance", "ArticlePerformance", "ArticlePerformanceCreate"),
        "editorial_author_performance": ("editorial.author_performance", "AuthorPerformance", "AuthorPerformanceCreate"),
        "editorial_category_performance": ("editorial.category_performance", "CategoryPerformance", "CategoryPerformanceCreate"),
        "editorial_content_events": ("editorial.content_event", "ContentEvent", "ContentEventCreate"),
        "editorial_headline_tests": ("editorial.headline_test", "HeadlineTest", "HeadlineTestCreate"),
        "editorial_trending_topics": ("editorial.trending_topic", "TrendingTopic", "TrendingTopicCreate"),
        "editorial_content_recommendations": ("editorial.content_recommendation", "ContentRecommendation", "ContentRecommendationCreate"),
        "editorial_article_content": ("editorial.article_content", "ArticleContent", "ArticleContentCreate"),
        "editorial_content_versions": ("editorial.content_version", "ContentVersion", "ContentVersionCreate"),
        "editorial_media_assets": ("editorial.media_asset", "MediaAsset", "MediaAssetCreate"),
        "editorial_media_variants": ("editorial.media_variant", "MediaVariant", "MediaVariantCreate"),
        "editorial_content_media": ("editorial.content_media", "ContentMedia", "ContentMediaCreate"),
        "editorial_media_collections": ("editorial.media_collection", "MediaCollection", "MediaCollectionCreate"),
        "editorial_media_collection_items": ("editorial.media_collection_item", "MediaCollectionItem", "MediaCollectionItemCreate"),
        "editorial_media_usage": ("editorial.media_usage", "MediaUsage", "MediaUsageCreate"),
        
        # Company domain
        "company_departments": ("company.department", "Department", "DepartmentCreate"),
        "company_employees": ("company.employee", "Employee", "EmployeeCreate"),
        "company_internal_content": ("company.internal_content", "InternalContent", "InternalContentCreate"),
        "company_content_performance": ("company.content_performance", "ContentPerformance", "ContentPerformanceCreate"),
        "company_department_performance": ("company.department_performance", "DepartmentPerformance", "DepartmentPerformanceCreate"),
        "company_content_events": ("company.content_event", "ContentEvent", "ContentEventCreate"),
        "company_employee_engagement": ("company.employee_engagement", "EmployeeEngagement", "EmployeeEngagementCreate"),
        "company_communications_analytics": ("company.communications_analytics", "CommunicationsAnalytics", "CommunicationsAnalyticsCreate"),
        
        # Security domain
        "security_roles": ("security.role", "Role", "RoleCreate"),
        "security_permissions": ("security.permission", "Permission", "PermissionCreate"),
        "security_role_permissions": ("security.role_permission", "RolePermission", "RolePermissionCreate"),
        "security_user_roles": ("security.user_role", "UserRole", "UserRoleCreate"),
        "security_api_keys": ("security.api_key", "APIKey", "APIKeyCreate"),
        "security_api_key_usage": ("security.api_key_usage", "APIKeyUsage", "APIKeyUsageCreate"),
        "security_user_sessions": ("security.user_session", "UserSession", "UserSessionCreate"),
        
        # Audit domain
        "audit_logs": ("audit.audit_log", "AuditLog", "AuditLogCreate"),
        "audit_data_changes": ("audit.data_change", "DataChange", "DataChangeCreate"),
        "audit_data_access": ("audit.data_access", "DataAccess", "DataAccessCreate"),
        "audit_security_events": ("audit.security_event", "SecurityEvent", "SecurityEventCreate"),
        "audit_data_lineage": ("audit.data_lineage", "DataLineage", "DataLineageCreate"),
        "audit_compliance_events": ("audit.compliance_event", "ComplianceEvent", "ComplianceEventCreate"),
        
        # Compliance domain
        "compliance_user_consent": ("compliance.user_consent", "UserConsent", "UserConsentCreate"),
        "compliance_data_subject_requests": ("compliance.data_subject_request", "DataSubjectRequest", "DataSubjectRequestCreate"),
        "compliance_retention_policies": ("compliance.retention_policy", "RetentionPolicy", "RetentionPolicyCreate"),
        "compliance_retention_executions": ("compliance.retention_execution", "RetentionExecution", "RetentionExecutionCreate"),
        "compliance_anonymized_data": ("compliance.anonymized_data", "AnonymizedData", "AnonymizedDataCreate"),
        "compliance_privacy_assessments": ("compliance.privacy_assessment", "PrivacyAssessment", "PrivacyAssessmentCreate"),
        "compliance_breach_incidents": ("compliance.breach_incident", "BreachIncident", "BreachIncidentCreate"),
        
        # ML Models domain
        "ml_model_registry": ("ml_models.model_registry", "ModelRegistry", "ModelRegistryCreate"),
        "ml_model_features": ("ml_models.model_feature", "ModelFeature", "ModelFeatureCreate"),
        "ml_model_training_runs": ("ml_models.training_run", "TrainingRun", "TrainingRunCreate"),
        "ml_model_predictions": ("ml_models.model_prediction", "ModelPrediction", "ModelPredictionCreate"),
        "ml_model_monitoring": ("ml_models.model_monitoring", "ModelMonitoring", "ModelMonitoringCreate"),
        "ml_model_ab_tests": ("ml_models.model_ab_test", "ModelABTest", "ModelABTestCreate"),
        
        # Data Quality domain
        "data_quality_rules": ("data_quality.quality_rule", "QualityRule", "QualityRuleCreate"),
        "data_quality_checks": ("data_quality.quality_check", "QualityCheck", "QualityCheckCreate"),
        "data_quality_metrics": ("data_quality.quality_metric", "QualityMetric", "QualityMetricCreate"),
        "data_quality_alerts": ("data_quality.quality_alert", "QualityAlert", "QualityAlertCreate"),
        "data_validation_results": ("data_quality.validation_result", "ValidationResult", "ValidationResultCreate"),
        
        # Configuration domain
        "configuration_feature_flags": ("configuration.feature_flag", "FeatureFlag", "FeatureFlagCreate"),
        "configuration_feature_flag_history": ("configuration.feature_flag_history", "FeatureFlagHistory", "FeatureFlagHistoryCreate"),
        "configuration_system_settings": ("configuration.system_setting", "SystemSetting", "SystemSettingCreate"),
        "configuration_system_settings_history": ("configuration.system_setting_history", "SystemSettingHistory", "SystemSettingHistoryCreate"),
    }
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """Initialize generic Pydantic model transformer."""
        super().__init__(spark, config)
        self.table_name = self.config.get("table_name")
        self.validate_with_model = self.config.get("validate_with_model", True)
        self.model_class = None
        self.create_model_class = None
        
        # Load model classes if table name is provided
        if self.table_name:
            self._load_model_classes()
    
    def _load_model_classes(self) -> None:
        """Load Pydantic model classes based on table name."""
        if not self.table_name:
            return
        
        normalized_name = self.table_name.lower().strip()
        model_info = self.MODEL_MODULE_MAP.get(normalized_name)
        
        if not model_info:
            logger.debug(f"No Pydantic model found for table '{self.table_name}'")
            return
        
        module_path, read_model_name, create_model_name = model_info
        
        try:
            # Dynamically import the model module
            from importlib import import_module
            model_module = import_module(f"src.ownlens.models.{module_path}")
            
            # Get the model classes
            self.model_class = getattr(model_module, read_model_name, None)
            self.create_model_class = getattr(model_module, create_model_name, None)
            
            if self.model_class:
                logger.info(
                    f"Loaded Pydantic model '{read_model_name}' for table '{self.table_name}'"
                )
            else:
                logger.warning(
                    f"Model class '{read_model_name}' not found in module 'src.ownlens.models.{module_path}'"
                )
        except ImportError as e:
            logger.warning(
                f"Failed to import model module 'src.ownlens.models.{module_path}': {e}"
            )
        except AttributeError as e:
            logger.warning(
                f"Model class '{read_model_name}' or '{create_model_name}' not found: {e}"
            )
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform data with Pydantic validation.
        
        Steps:
        1. Load model classes if table name provided in kwargs
        2. Clean nulls in required fields
        3. Transform to match model schema
        4. Validate against Pydantic model (if enabled)
        
        Args:
            df: Input DataFrame
            **kwargs: Transform parameters
                - table_name: Name of table (for model lookup)
                - validate: Whether to validate against Pydantic model (default: True)
                - filter_invalid: Whether to filter out invalid rows (default: True)
            
        Returns:
            Transformed DataFrame
        """
        # Update table name from kwargs if provided
        table_name = kwargs.get("table_name") or self.table_name
        if table_name and table_name != self.table_name:
            self.table_name = table_name
            self._load_model_classes()
        
        # If no model class, fall back to base transformation
        if not self.model_class:
            logger.debug(f"No Pydantic model for table '{table_name}', using base transformation")
            return super().transform(df, **kwargs)
        
        # Step 1: Clean nulls in required fields (get from model)
        # For now, we'll do basic cleaning
        df = self.clean_nulls(df)
        
        # Step 2: Transform to match model schema
        if self.validate_with_model and self.model_class:
            df = ModelValidator.transform_to_model_schema(df, self.model_class)
        
        # Step 3: Validate against Pydantic model
        validate = kwargs.get("validate", self.validate_with_model)
        filter_invalid = kwargs.get("filter_invalid", True)
        
        if validate and self.model_class:
            try:
                valid_df, invalid_df = ModelValidator.validate_dataframe(
                    df,
                    model_class=self.model_class,
                    strict=False,
                    filter_invalid=filter_invalid
                )
                
                if invalid_df.count() > 0:
                    invalid_count = invalid_df.count()
                    logger.warning(
                        f"Filtered out {invalid_count} invalid rows for table '{table_name}'"
                    )
                
                # Add processing timestamp
                df = self.add_timestamp(valid_df)
                
                return df
            except Exception as e:
                logger.error(
                    f"Validation failed for table '{table_name}': {e}",
                    exc_info=True
                )
                # Return original DataFrame if validation fails
                return df
        
        # Add processing timestamp
        df = self.add_timestamp(df)
        
        return df

