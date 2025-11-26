# OwnLens Models

Comprehensive Pydantic-based validation models for all OwnLens domains.

## Structure

```
src/ownlens/models/
├── __init__.py                 # Package initialization
├── base.py                     # Base model classes and mixins
├── field_validations.py        # Common field validators
├── README.md                   # This file
│
├── customer/                   # Customer domain models
│   ├── __init__.py
│   ├── user_event.py           # User event validation
│   ├── session.py              # Session validation
│   ├── user.py                 # User validation
│   ├── user_features.py        # ML user features validation
│   ├── user_segment.py         # User segmentation validation
│   ├── churn_prediction.py     # Churn prediction validation
│   ├── recommendation.py       # Content recommendation validation
│   └── conversion_prediction.py # Conversion prediction validation
│
├── editorial/                  # Editorial domain models
│   ├── __init__.py
│   ├── article.py              # Article validation
│   ├── author.py               # Author validation
│   ├── article_content.py      # Article content validation
│   ├── content_version.py      # Content versioning validation
│   ├── media_asset.py          # Media asset validation
│   ├── media_variant.py        # Media variant validation
│   ├── content_media.py        # Content-media relationship validation
│   ├── article_performance.py  # Article performance validation
│   ├── author_performance.py   # Author performance validation
│   └── category_performance.py # Category performance validation
│
└── company/                    # Company domain models
    ├── __init__.py
    ├── department.py           # Department validation
    ├── employee.py             # Employee validation
    ├── internal_content.py     # Internal content validation
    ├── content_performance.py  # Content performance validation
    ├── department_performance.py # Department performance validation
    ├── employee_engagement.py  # Employee engagement validation
    └── communications_analytics.py # Communications analytics validation
```

## Base Classes

### `BaseSchema`
Base model class with common Pydantic configuration:
- Enum value serialization
- Assignment validation
- Field name population
- JSON encoders for datetime and UUID
- Extra field handling (forbid)

### `TimestampMixin`
Mixin for timestamp fields:
- `created_at`: Creation timestamp
- `updated_at`: Last update timestamp (auto-set)

### `MetadataMixin`
Mixin for metadata fields:
- `metadata`: Additional metadata dictionary

### `CompanyBrandMixin`
Mixin for company and brand scoping:
- `company_id`: Company ID
- `brand_id`: Brand ID
- UUID validation

### `BaseEntity`
Base entity with common fields:
- `id`: Entity ID (UUID)
- Inherits from `BaseSchema`, `TimestampMixin`, `MetadataMixin`

### `BaseEntityWithCompanyBrand`
Base entity with company and brand scoping:
- Inherits from `BaseEntity` and `CompanyBrandMixin`

## Field Validators

The `FieldValidators` class provides comprehensive validation utilities:

### UUID Validation
- `validate_uuid()`: Validate UUID format

### String Validation
- `validate_email()`: Email format validation
- `validate_url()`: URL format validation
- `validate_country_code()`: ISO 3166-1 alpha-2 country code
- `validate_language_code()`: ISO 639-1 language code
- `validate_timezone()`: Timezone format validation
- `validate_phone_number()`: Phone number format validation
- `validate_string_length()`: String length validation
- `validate_code()`: Code format (alphanumeric, underscores, hyphens)
- `validate_slug()`: Slug format (lowercase, hyphens)

### Date/Time Validation
- `validate_date()`: Date format validation
- `validate_datetime()`: Datetime format validation
- `validate_timestamp()`: Unix timestamp validation
- `validate_iso_date()`: ISO 8601 date format
- `validate_iso_datetime()`: ISO 8601 datetime format

### Numeric Validation
- `validate_percentage()`: Percentage (0.0 to 1.0)
- `validate_positive_integer()`: Positive integer
- `validate_non_negative_integer()`: Non-negative integer
- `validate_positive_float()`: Positive float
- `validate_decimal()`: Decimal with precision

### Enum/Array Validation
- `validate_enum()`: Enum value validation
- `validate_array()`: Array length validation
- `validate_json()`: JSON object validation

### File/Media Validation
- `validate_file_extension()`: File extension validation
- `validate_mime_type()`: MIME type validation

### Other Validation
- `validate_hex_color()`: Hex color code validation
- `validate_ip_address()`: IP address validation (IPv4/IPv6)
- `validate_user_agent()`: User agent string validation
- `validate_status()`: Status value validation
- `validate_boolean()`: Boolean value validation
- `validate_currency_code()`: ISO 4217 currency code

## Model Patterns

Each domain model follows a consistent pattern:

### Base Model
- `{Entity}Base`: Base model with all fields
- Inherits from `BaseEntityWithCompanyBrand` or `BaseEntity`
- Field validators for type-specific validation

### Read Model
- `{Entity}`: Read model (inherits from base)
- Used for API responses

### Create Model
- `{Entity}Create`: Creation model
- Auto-generates IDs if not provided
- Sets default timestamps

### Update Model
- `{Entity}Update`: Update model
- All fields optional
- Only includes updatable fields

## Usage Examples

### Customer Domain

```python
from src.ownlens.models.customer import UserEventCreate, UserEvent

# Create a user event
event = UserEventCreate(
    user_id="123e4567-e89b-12d3-a456-426614174000",
    session_id="123e4567-e89b-12d3-a456-426614174001",
    event_type="article_view",
    event_timestamp=datetime.utcnow(),
    article_id="article-123",
    country_code="US",
    engagement_metrics={"scroll_depth": 75, "time_on_page": 120}
)

# Validate
event.model_validate(event.dict())
```

### Editorial Domain

```python
from src.ownlens.models.editorial import ArticleCreate, Article

# Create an article
article = ArticleCreate(
    title="Breaking News: Important Event",
    headline="Important Event Happens",
    content_url="https://example.com/article/123",
    article_type="news",
    primary_author_id="123e4567-e89b-12d3-a456-426614174000",
    primary_category_id="123e4567-e89b-12d3-a456-426614174001",
    publish_time=datetime.utcnow(),
    status="published"
)

# Validate
article.model_validate(article.dict())
```

### Company Domain

```python
from src.ownlens.models.company import EmployeeCreate, Employee

# Create an employee
employee = EmployeeCreate(
    first_name="John",
    last_name="Doe",
    email="john.doe@example.com",
    department_id="123e4567-e89b-12d3-a456-426614174000",
    job_title="Software Engineer",
    employment_type="full_time",
    employment_status="active",
    hire_date=date.today()
)

# Validate
employee.model_validate(employee.dict())
```

## Validation Features

### Automatic Validation
- All fields are validated on model creation
- Type coercion where appropriate
- Custom validators for complex fields

### Error Messages
- Clear, descriptive error messages
- Field-specific validation errors
- Enum value suggestions

### Data Transformation
- Automatic UUID generation
- Timestamp defaults
- String normalization (trim, lowercase)
- Enum value normalization

## Integration

These schemas are designed to work with:
- **FastAPI**: Request/response models
- **SQLAlchemy**: ORM model validation
- **Pydantic**: Core validation engine
- **PostgreSQL**: Database schema alignment

## Best Practices

1. **Always use Create models for input**: They handle ID generation and defaults
2. **Use Read models for output**: They include all fields for API responses
3. **Use Update models for partial updates**: All fields are optional
4. **Validate early**: Validate data as soon as it enters the system
5. **Handle validation errors**: Provide clear error messages to users

## Extending

To add new validators:

1. Add validator method to `FieldValidators` class in `field_validations.py`
2. Add convenience function at module level
3. Use in domain models as needed

To add new models:

1. Create new file in appropriate domain directory
2. Follow the pattern: Base, Read, Create, Update
3. Import and export in `__init__.py`
4. Add validators for domain-specific fields

## Dependencies

- `pydantic>=2.0.0`: Core validation framework
- `python-dateutil`: Date/time parsing (optional)
- Standard library: `datetime`, `uuid`, `typing`, `re`

