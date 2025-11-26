# Complete Service Mapping - OwnLens

## Analysis: Repositories That Need Services

### ✅ Repositories That NEED Services (Independent Entities)

These are independent entities with business logic that require full CRUD operations:

#### Customer Domain (3 missing)
- ✅ `ChurnPredictionRepository` → `ChurnPredictionService`
- ✅ `RecommendationRepository` → `RecommendationService`
- ✅ `ConversionPredictionRepository` → `ConversionPredictionService`

#### Editorial Domain (6 missing)
- ✅ `MediaCollectionRepository` → `MediaCollectionService`
- ✅ `MediaVariantRepository` → `MediaVariantService`
- ✅ `ContentVersionRepository` → `ContentVersionService`
- ✅ `HeadlineTestRepository` → `HeadlineTestService`
- ✅ `TrendingTopicRepository` → `TrendingTopicService`
- ✅ `ContentRecommendationRepository` → `ContentRecommendationService`

#### Compliance Domain (3 missing)
- ✅ `BreachIncidentRepository` → `BreachIncidentService`
- ✅ `PrivacyAssessmentRepository` → `PrivacyAssessmentService`
- ✅ `AnonymizedDataRepository` → `AnonymizedDataService`
- ✅ `RetentionExecutionRepository` → `RetentionExecutionService`

#### Audit Domain (2 missing)
- ✅ `DataLineageRepository` → `DataLineageService`
- ✅ `DataAccessRepository` → `DataAccessService`
- ✅ `ComplianceEventRepository` → `ComplianceEventService`

#### Data Quality Domain (2 missing)
- ✅ `QualityAlertRepository` → `QualityAlertService`
- ✅ `ValidationResultRepository` → `ValidationResultService`

#### ML Models Domain (3 missing)
- ✅ `ModelFeatureRepository` → `ModelFeatureService`
- ✅ `ModelMonitoringRepository` → `ModelMonitoringService`
- ✅ `ModelABTestRepository` → `ModelABTestService`

#### Security Domain (1 missing)
- ✅ `UserSessionRepository` → `UserSessionService`

**Total Missing Services: 20**

---

### ❌ Repositories That DON'T Need Services

These are join tables, history tables, reference data, or analytics that don't need dedicated services:

#### Join Tables (5) - Handled by parent services
- `BrandCountryRepository` → Handled by `BrandService`
- `RolePermissionRepository` → Handled by `RoleService`
- `UserRoleRepository` → Handled by `RoleService` or `UserService`
- `ContentMediaRepository` → Handled by `ArticleService` or `MediaAssetService`
- `MediaCollectionItemRepository` → Handled by `MediaCollectionService`

#### History Tables (2) - Handled by parent services
- `FeatureFlagHistoryRepository` → Handled by `FeatureFlagService`
- `SystemSettingHistoryRepository` → Handled by `SystemSettingService`

#### Reference Data (5) - Read-only, accessed via repositories
- `CountryRepository` → Reference data, no service needed
- `CityRepository` → Reference data, no service needed
- `DeviceTypeRepository` → Reference data, no service needed
- `OperatingSystemRepository` → Reference data, no service needed
- `BrowserRepository` → Reference data, no service needed

#### Analytics/Performance (8) - Read-only analytics, handled by parent services
- `ArticlePerformanceRepository` → Handled by `ArticleService`
- `AuthorPerformanceRepository` → Handled by `AuthorService`
- `CategoryPerformanceRepository` → Handled by `CategoryService`
- `ContentPerformanceRepository` → Handled by `InternalContentService`
- `DepartmentPerformanceRepository` → Handled by `DepartmentService`
- `EmployeeEngagementRepository` → Handled by `EmployeeService`
- `CommunicationsAnalyticsRepository` → Handled by `InternalContentService`
- `MediaUsageRepository` → Handled by `MediaAssetService`

#### Event Tracking (3) - Append-only logs, handled by parent services
- `ContentEventRepository` (editorial) → Handled by `ArticleService`
- `ContentEventRepository` (company) → Handled by `InternalContentService`
- `ApiKeyUsageRepository` → Handled by `ApiKeyService`

#### User Account (1) - Handled by UserService
- `UserAccountRepository` → Handled by `UserService`

**Total Repositories That Don't Need Services: 24**

---

## Summary

| Category | Count | Status |
|----------|-------|--------|
| **Services Created** | 32 | ✅ |
| **Services Needed** | 20 | ⚠️ Missing |
| **Repositories Without Services** | 24 | ✅ (Don't need) |
| **Total Repositories** | 78 | ✅ |
| **Total Services Needed** | **52** | ✅ |

---

## Action Plan

Create 20 missing services for independent entities:
1. Customer: 3 services (ChurnPrediction, Recommendation, ConversionPrediction)
2. Editorial: 6 services (MediaCollection, MediaVariant, ContentVersion, HeadlineTest, TrendingTopic, ContentRecommendation)
3. Compliance: 4 services (BreachIncident, PrivacyAssessment, AnonymizedData, RetentionExecution)
4. Audit: 3 services (DataLineage, DataAccess, ComplianceEvent)
5. Data Quality: 2 services (QualityAlert, ValidationResult)
6. ML Models: 3 services (ModelFeature, ModelMonitoring, ModelABTest)
7. Security: 1 service (UserSession)

**Total: 20 services to create**








