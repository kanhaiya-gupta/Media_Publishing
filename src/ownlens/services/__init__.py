"""
OwnLens Service Module
======================

Business logic services for OwnLens.
"""

# Shared Infrastructure
from .shared import (
    BaseService,
    ServiceError,
    ValidationError,
    NotFoundError,
    ConflictError,
    ValidationService,
    NotificationService,
    AuditService,
    CacheService,
)

# Service Factory
from .service_factory import ServiceFactory

# Base Domain Services
from .base import (
    CompanyService,
    BrandService,
    UserService,
    CategoryService,
)

# Customer Domain Services
from .customer import (
    SessionService,
    UserEventService,
    UserFeaturesService,
    UserSegmentService,
    ChurnPredictionService,
    RecommendationService,
    ConversionPredictionService,
)

# Editorial Domain Services
from .editorial import (
    ArticleService,
    AuthorService,
    ArticleContentService,
    MediaAssetService,
    MediaCollectionService,
    MediaVariantService,
    ContentVersionService,
    HeadlineTestService,
    TrendingTopicService,
    ContentRecommendationService,
)

# Company Domain Services
from .company import (
    DepartmentService,
    EmployeeService,
    InternalContentService,
)

# Security Domain Services
from .security import (
    RoleService,
    PermissionService,
    ApiKeyService,
    UserSessionService,
)

# Compliance Domain Services
from .compliance import (
    UserConsentService,
    DataSubjectRequestService,
    RetentionPolicyService,
    BreachIncidentService,
    PrivacyAssessmentService,
    AnonymizedDataService,
    RetentionExecutionService,
)

# Audit Domain Services
from .audit import (
    AuditLogService,
    DataChangeService,
    SecurityEventService,
    DataLineageService,
    DataAccessService,
    ComplianceEventService,
)

# Data Quality Domain Services
from .data_quality import (
    QualityRuleService,
    QualityCheckService,
    QualityMetricService,
    QualityAlertService,
    ValidationResultService,
)

# ML Models Domain Services
from .ml_models import (
    ModelRegistryService,
    TrainingRunService,
    ModelPredictionService,
    ModelFeatureService,
    ModelMonitoringService,
    ModelABTestService,
)

# Configuration Domain Services
from .configuration import (
    FeatureFlagService,
    SystemSettingService,
)

__all__ = [
    # Shared Infrastructure
    "BaseService",
    "ServiceError",
    "ValidationError",
    "NotFoundError",
    "ConflictError",
    "ValidationService",
    "NotificationService",
    "AuditService",
    "CacheService",
    
    # Service Factory
    "ServiceFactory",
    
    # Base Domain Services
    "CompanyService",
    "BrandService",
    "UserService",
    "CategoryService",
    
    # Customer Domain Services
    "SessionService",
    "UserEventService",
    "UserFeaturesService",
    "UserSegmentService",
    "ChurnPredictionService",
    "RecommendationService",
    "ConversionPredictionService",
    
    # Editorial Domain Services
    "ArticleService",
    "AuthorService",
    "ArticleContentService",
    "MediaAssetService",
    "MediaCollectionService",
    "MediaVariantService",
    "ContentVersionService",
    "HeadlineTestService",
    "TrendingTopicService",
    "ContentRecommendationService",
    
    # Company Domain Services
    "DepartmentService",
    "EmployeeService",
    "InternalContentService",
    
    # Security Domain Services
    "RoleService",
    "PermissionService",
    "ApiKeyService",
    "UserSessionService",
    
    # Compliance Domain Services
    "UserConsentService",
    "DataSubjectRequestService",
    "RetentionPolicyService",
    "BreachIncidentService",
    "PrivacyAssessmentService",
    "AnonymizedDataService",
    "RetentionExecutionService",
    
    # Audit Domain Services
    "AuditLogService",
    "DataChangeService",
    "SecurityEventService",
    "DataLineageService",
    "DataAccessService",
    "ComplianceEventService",
    
    # Data Quality Domain Services
    "QualityRuleService",
    "QualityCheckService",
    "QualityMetricService",
    "QualityAlertService",
    "ValidationResultService",
    
    # ML Models Domain Services
    "ModelRegistryService",
    "TrainingRunService",
    "ModelPredictionService",
    "ModelFeatureService",
    "ModelMonitoringService",
    "ModelABTestService",
    
    # Configuration Domain Services
    "FeatureFlagService",
    "SystemSettingService",
]
