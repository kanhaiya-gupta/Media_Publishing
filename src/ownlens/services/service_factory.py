"""
Service Factory - OwnLens
==========================

Service factory for OwnLens.
Provides service instantiation, dependency injection, and lifecycle management.

This factory provides:
- Service instantiation and dependency management
- Repository injection and lifecycle management
- Service caching and singleton patterns
- Configuration management
- Service discovery and registration
"""

from typing import Dict, Any, Type, Optional, TypeVar, List
import logging
from datetime import datetime

from ..repositories.shared.repository_factory import RepositoryFactory
from .shared.base_service import BaseService
from .shared import (
    ValidationService,
    NotificationService,
    AuditService,
    CacheService,
)

logger = logging.getLogger(__name__)

# Type variable for service typing
ServiceT = TypeVar('ServiceT', bound=BaseService)


class ServiceFactory:
    """
    Service factory for OwnLens.
    
    Provides service instantiation, dependency injection, and lifecycle management
    for all OwnLens services. Supports caching, configuration, and service discovery.
    """
    
    def __init__(self, connection_manager=None, repository_factory: RepositoryFactory = None):
        """
        Initialize the service factory.
        
        Args:
            connection_manager: Database connection manager (optional, will try to get from database manager if not provided)
            repository_factory: Optional repository factory instance
        """
        # Connection manager must be provided
        if connection_manager is None:
            self.logger = logging.getLogger(f"{__name__}.ServiceFactory")
            self.logger.warning("Connection manager not provided. Services may not work without database.")
        
        self.connection_manager = connection_manager
        self.repository_factory = repository_factory or (RepositoryFactory(connection_manager) if connection_manager else None)
        self.logger = logging.getLogger(f"{__name__}.ServiceFactory")
        
        # Service registry and cache
        self._services: Dict[str, BaseService] = {}
        self._service_classes: Dict[str, Type[BaseService]] = {}
        self._service_configs: Dict[str, Dict[str, Any]] = {}
        
        # Shared services
        self._shared_services = {
            'validation': ValidationService(),
            'notification': NotificationService(),
            'audit': AuditService(),
            'cache': CacheService()
        }
        
        # Factory configuration
        self._config = {
            'enable_caching': True,
            'enable_singleton': True,
            'auto_initialize': True,
            'lazy_loading': True
        }
        
        self.logger.info("Service factory initialized")
    
    # ==================== CONFIGURATION METHODS ====================
    
    def configure(self, **kwargs) -> None:
        """
        Configure factory settings.
        
        Args:
            **kwargs: Configuration options
        """
        self._config.update(kwargs)
        self.logger.info(f"Service factory configured with: {kwargs}")
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self._config.get(key, default)
    
    # ==================== SERVICE REGISTRATION METHODS ====================
    
    def register_service(self, service_name: str, service_class: Type[ServiceT], config: Dict[str, Any] = None) -> None:
        """
        Register a service class with the factory.
        
        Args:
            service_name: Name to register the service under
            service_class: Service class to register
            config: Optional service configuration
        """
        self._service_classes[service_name] = service_class
        self._service_configs[service_name] = config or {}
        
        self.logger.info(f"Registered service: {service_name} -> {service_class.__name__}")
    
    def register_services(self, services: Dict[str, Type[ServiceT]]) -> None:
        """
        Register multiple services at once.
        
        Args:
            services: Dictionary mapping service names to service classes
        """
        for service_name, service_class in services.items():
            self.register_service(service_name, service_class)
    
    def unregister_service(self, service_name: str) -> None:
        """
        Unregister a service from the factory.
        
        Args:
            service_name: Name of the service to unregister
        """
        if service_name in self._service_classes:
            del self._service_classes[service_name]
            if service_name in self._service_configs:
                del self._service_configs[service_name]
            if service_name in self._services:
                del self._services[service_name]
            
            self.logger.info(f"Unregistered service: {service_name}")
    
    # ==================== SERVICE INSTANTIATION METHODS ====================
    
    async def get_service(self, service_name: str, force_new: bool = False) -> Optional[BaseService]:
        """
        Get or create a service instance.
        
        Args:
            service_name: Name of the service to get
            force_new: Force creation of a new instance (bypass cache)
            
        Returns:
            Service instance or None if not found
        """
        try:
            # Check if service is already cached and caching is enabled
            if (not force_new and 
                self.get_config('enable_caching', True) and 
                service_name in self._services):
                self.logger.debug(f"Returning cached service: {service_name}")
                return self._services[service_name]
            
            # Check if service class is registered
            if service_name not in self._service_classes:
                self.logger.error(f"Service not registered: {service_name}")
                return None
            
            # Get service class and configuration
            service_class = self._service_classes[service_name]
            service_config = self._service_configs.get(service_name, {})
            
            # Get repository for the service
            repository = await self._get_repository_for_service(service_name, service_class)
            if not repository:
                self.logger.error(f"Could not get repository for service: {service_name}")
                return None
            
            # Create service instance
            service_instance = service_class(repository, service_name)
            
            # Apply service configuration
            if service_config:
                service_instance.configure(**service_config)
            
            # Cache the service if caching is enabled
            if self.get_config('enable_caching', True):
                self._services[service_name] = service_instance
                self.logger.debug(f"Cached service: {service_name}")
            
            self.logger.info(f"Created service: {service_name}")
            return service_instance
            
        except Exception as e:
            self.logger.error(f"Error creating service {service_name}: {str(e)}", exc_info=True)
            return None
    
    async def create_service(self, service_class: Type[ServiceT], service_name: str = None, config: Dict[str, Any] = None) -> Optional[ServiceT]:
        """
        Create a new service instance directly.
        
        Args:
            service_class: Service class to instantiate
            service_name: Optional service name
            config: Optional service configuration
            
        Returns:
            Service instance or None if creation failed
        """
        try:
            service_name = service_name or service_class.__name__
            
            # Get repository for the service
            repository = await self._get_repository_for_service(service_name, service_class)
            if not repository:
                self.logger.error(f"Could not get repository for service: {service_name}")
                return None
            
            # Create service instance
            service_instance = service_class(repository, service_name)
            
            # Apply configuration
            if config:
                service_instance.configure(**config)
            
            self.logger.info(f"Created service directly: {service_name}")
            return service_instance
            
        except Exception as e:
            self.logger.error(f"Error creating service {service_name}: {str(e)}", exc_info=True)
            return None
    
    async def _get_repository_for_service(self, service_name: str, service_class: Type[ServiceT]) -> Optional[Any]:
        """
        Get the appropriate repository for a service.
        
        Args:
            service_name: Name of the service
            service_class: Service class
            
        Returns:
            Repository instance or None if not found
        """
        try:
            if not self.repository_factory:
                self.logger.error("Repository factory not initialized")
                return None
            
            # Try to determine repository type from service class
            repository_type = self._determine_repository_type(service_class)
            
            if repository_type:
                # Try to get repository by type name (e.g., 'CompanyRepository')
                repository = self.repository_factory.get_repository(repository_type)
                if repository:
                    return repository
                
                # Fallback: try to get by service name
                return self.repository_factory.get_repository(service_name)
            else:
                # Fallback: try to get repository by service name
                return self.repository_factory.get_repository(service_name)
                
        except Exception as e:
            self.logger.error(f"Error getting repository for service {service_name}: {str(e)}")
            return None
    
    def _determine_repository_type(self, service_class: Type[ServiceT]) -> Optional[str]:
        """
        Determine the repository type needed for a service class.
        
        Args:
            service_class: Service class
            
        Returns:
            Repository type name or None if cannot determine
        """
        service_name = service_class.__name__.lower()
        
        # Map service names to repository types for OwnLens
        repository_mapping = {
            # Base domain services
            'companyservice': 'CompanyRepository',
            'brandservice': 'BrandRepository',
            'userservice': 'UserRepository',
            'categoryservice': 'CategoryRepository',
            
            # Customer domain services
            'sessionservice': 'SessionRepository',
            'usereventservice': 'UserEventRepository',
            'userfeaturesservice': 'UserFeaturesRepository',
            'usersegmentservice': 'UserSegmentRepository',
            'churnpredictionservice': 'ChurnPredictionRepository',
            'recommendationservice': 'RecommendationRepository',
            'conversionpredictionservice': 'ConversionPredictionRepository',
            
            # Editorial domain services
            'articleservice': 'ArticleRepository',
            'authorservice': 'AuthorRepository',
            'articlecontentservice': 'ArticleContentRepository',
            'mediaassetservice': 'MediaAssetRepository',
            'mediacollectionservice': 'MediaCollectionRepository',
            'mediavariantservice': 'MediaVariantRepository',
            'contentversionservice': 'ContentVersionRepository',
            'headlinetestservice': 'HeadlineTestRepository',
            'trendingtopicservice': 'TrendingTopicRepository',
            'contentrecommendationservice': 'ContentRecommendationRepository',
            
            # Company domain services
            'departmentservice': 'DepartmentRepository',
            'employeeservice': 'EmployeeRepository',
            'internalcontentservice': 'InternalContentRepository',
            
            # Security domain services
            'roleservice': 'RoleRepository',
            'permissionservice': 'PermissionRepository',
            'apikeyservice': 'ApiKeyRepository',
            'usersessionservice': 'UserSessionRepository',
            
            # Compliance domain services
            'userconsentservice': 'UserConsentRepository',
            'datasubjectrequestservice': 'DataSubjectRequestRepository',
            'retentionpolicyservice': 'RetentionPolicyRepository',
            'breachincidentservice': 'BreachIncidentRepository',
            'privacyassessmentservice': 'PrivacyAssessmentRepository',
            'anonymizeddataservice': 'AnonymizedDataRepository',
            'retentionexecutionservice': 'RetentionExecutionRepository',
            
            # Audit domain services
            'auditlogservice': 'AuditLogRepository',
            'datachangeservice': 'DataChangeRepository',
            'securityeventservice': 'SecurityEventRepository',
            'datalineageservice': 'DataLineageRepository',
            'dataaccessservice': 'DataAccessRepository',
            'complianceeventservice': 'ComplianceEventRepository',
            
            # Data Quality domain services
            'qualityruleservice': 'QualityRuleRepository',
            'qualitycheckservice': 'QualityCheckRepository',
            'qualitymetricservice': 'QualityMetricRepository',
            'qualityalertservice': 'QualityAlertRepository',
            'validationresultservice': 'ValidationResultRepository',
            
            # ML Models domain services
            'modelregistryservice': 'ModelRegistryRepository',
            'trainingrunservice': 'TrainingRunRepository',
            'modelpredictionservice': 'ModelPredictionRepository',
            'modelfeatureservice': 'ModelFeatureRepository',
            'modelmonitoringservice': 'ModelMonitoringRepository',
            'modelabtestservice': 'ModelABTestRepository',
            
            # Configuration domain services
            'featureflagservice': 'FeatureFlagRepository',
            'systemsettingservice': 'SystemSettingRepository',
        }
        
        return repository_mapping.get(service_name)
    
    # ==================== SHARED SERVICES METHODS ====================
    
    def get_shared_service(self, service_name: str):
        """
        Get a shared service instance.
        
        Args:
            service_name: Name of the shared service (validation, notification, audit, cache)
            
        Returns:
            Shared service instance or None if not found
        """
        return self._shared_services.get(service_name)
    
    def get_shared_services(self) -> Dict[str, Any]:
        """
        Get all shared service instances.
        
        Returns:
            Dictionary of shared services
        """
        return self._shared_services.copy()
    
    # ==================== SERVICE MANAGEMENT METHODS ====================
    
    async def initialize_all_services(self) -> Dict[str, bool]:
        """
        Initialize all registered services.
        
        Returns:
            Dictionary mapping service names to initialization success status
        """
        results = {}
        
        for service_name in self._service_classes:
            try:
                service = await self.get_service(service_name)
                if service:
                    # Perform health check
                    health = await service.health_check()
                    results[service_name] = health.get('status') == 'healthy'
                else:
                    results[service_name] = False
            except Exception as e:
                self.logger.error(f"Error initializing service {service_name}: {str(e)}")
                results[service_name] = False
        
        self.logger.info(f"Initialized {sum(results.values())}/{len(results)} services")
        return results
    
    async def clear_cache(self) -> None:
        """Clear service cache."""
        self._services.clear()
        self.logger.info("Service cache cleared")
    
    def get_registered_services(self) -> List[str]:
        """
        Get list of registered service names.
        
        Returns:
            List of registered service names
        """
        return list(self._service_classes.keys())
    
    def get_service_info(self) -> Dict[str, Any]:
        """
        Get service factory information.
        
        Returns:
            Dictionary containing factory information
        """
        return {
            "registered_services": len(self._service_classes),
            "cached_services": len(self._services),
            "shared_services": list(self._shared_services.keys()),
            "config": self._config,
            "timestamp": datetime.utcnow().isoformat()
        }

