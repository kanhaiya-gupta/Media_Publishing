"""
Validation Service - OwnLens
=============================

Validation service for OwnLens.
Provides comprehensive data validation and business rule validation.

This service provides:
- Data validation and sanitization
- Business rule validation
- Custom validation rules for OwnLens domains
- Validation error handling and reporting
- Input security validation
"""

from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime, date
import re
import logging
from enum import Enum

from pydantic import BaseModel, ValidationError as PydanticValidationError

logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """Validation levels for different contexts."""
    STRICT = "strict"
    MODERATE = "moderate"
    LENIENT = "lenient"


class ValidationRule:
    """Represents a validation rule."""
    
    def __init__(self, name: str, validator: Callable, error_message: str, 
                 required: bool = True, level: ValidationLevel = ValidationLevel.MODERATE):
        self.name = name
        self.validator = validator
        self.error_message = error_message
        self.required = required
        self.level = level


class ValidationResult:
    """Represents the result of a validation operation."""
    
    def __init__(self, is_valid: bool = True, errors: List[str] = None, warnings: List[str] = None):
        self.is_valid = is_valid
        self.errors = errors or []
        self.warnings = warnings or []
        self.timestamp = datetime.utcnow()
    
    def add_error(self, error: str) -> None:
        """Add a validation error."""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str) -> None:
        """Add a validation warning."""
        self.warnings.append(warning)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "timestamp": self.timestamp.isoformat()
        }


class ValidationService:
    """
    Validation service for OwnLens.
    
    Provides comprehensive data validation and business rule validation
    for all OwnLens domains and use cases.
    """
    
    def __init__(self):
        """Initialize the validation service."""
        self.logger = logging.getLogger(f"{__name__}.ValidationService")
        
        # Validation rules registry
        self._rules: Dict[str, List[ValidationRule]] = {}
        
        # Common validation patterns
        self._patterns = {
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'uuid': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE),
            'alphanumeric': re.compile(r'^[a-zA-Z0-9]+$'),
            'alphanumeric_underscore': re.compile(r'^[a-zA-Z0-9_]+$'),
            'numeric': re.compile(r'^\d+(\.\d+)?$'),
            'positive_numeric': re.compile(r'^\d+(\.\d+)?$'),
            'country_code': re.compile(r'^[A-Z]{2}$'),  # ISO 3166-1 alpha-2
            'language_code': re.compile(r'^[a-z]{2}(-[A-Z]{2})?$'),  # ISO 639-1 with optional region
            'url': re.compile(r'^https?://[^\s/$.?#].[^\s]*$'),
            'slug': re.compile(r'^[a-z0-9]+(?:-[a-z0-9]+)*$'),
            # OwnLens domain-specific patterns
            'company_code': re.compile(r'^[a-z0-9_]+$'),
            'brand_code': re.compile(r'^[a-z0-9_]+$'),
            'category_code': re.compile(r'^[a-z0-9_]+$'),
            'article_code': re.compile(r'^[a-z0-9-]+$'),
            'media_code': re.compile(r'^[a-z0-9_-]+$'),
        }
        
        # Initialize common validation rules
        self._initialize_common_rules()
    
    def _initialize_common_rules(self) -> None:
        """Initialize common validation rules."""
        # Email validation
        self.add_rule('email', ValidationRule(
            'email_format',
            lambda x: self._patterns['email'].match(x) is not None,
            'Invalid email format',
            required=True
        ))
        
        # UUID validation
        self.add_rule('uuid', ValidationRule(
            'uuid_format',
            lambda x: self._patterns['uuid'].match(x) is not None,
            'Invalid UUID format',
            required=True
        ))
        
        # Positive number validation
        self.add_rule('positive_number', ValidationRule(
            'positive_number',
            lambda x: self._validate_positive_number(x),
            'Must be a positive number',
            required=True
        ))
    
    # ==================== RULE MANAGEMENT METHODS ====================
    
    def add_rule(self, category: str, validator: Union[ValidationRule, Callable], 
                 error_message: str = None, name: str = None) -> None:
        """
        Add a validation rule to a category.
        
        Args:
            category: Rule category
            validator: Validation rule object or validator function
            error_message: Error message (required if validator is a function)
            name: Rule name (optional, defaults to category)
        """
        if category not in self._rules:
            self._rules[category] = []
        
        # If validator is already a ValidationRule, use it directly
        if isinstance(validator, ValidationRule):
            rule = validator
        else:
            # Create ValidationRule from function
            if error_message is None:
                error_message = f"Validation failed for {category}"
            if name is None:
                name = category
            
            rule = ValidationRule(
                name=name,
                validator=validator,
                error_message=error_message
            )
        
        self._rules[category].append(rule)
        self.logger.debug(f"Added validation rule: {category}.{rule.name}")
    
    def get_rules(self, category: str) -> List[ValidationRule]:
        """
        Get validation rules for a category.
        
        Args:
            category: Rule category
            
        Returns:
            List of validation rules
        """
        return self._rules.get(category, [])
    
    # ==================== VALIDATION METHODS ====================
    
    async def validate_data(self, data: Any, category: str = "general", 
                          level: ValidationLevel = ValidationLevel.MODERATE) -> ValidationResult:
        """
        Validate data using rules for a specific category.
        
        Args:
            data: Data to validate
            category: Validation category
            level: Validation level
            
        Returns:
            Validation result
        """
        result = ValidationResult()
        
        try:
            # Get rules for the category
            rules = self.get_rules(category)
            
            if not rules:
                self.logger.warning(f"No validation rules found for category: {category}")
                return result
            
            # Apply rules
            for rule in rules:
                if rule.level.value == level.value or level == ValidationLevel.STRICT:
                    try:
                        if rule.required or data is not None:
                            if not rule.validator(data):
                                result.add_error(rule.error_message)
                    except Exception as e:
                        result.add_error(f"Validation error for {rule.name}: {str(e)}")
            
            self.logger.debug(f"Validation completed for category {category}: {result.is_valid}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error during validation: {str(e)}")
            result.add_error(f"Validation error: {str(e)}")
            return result
    
    async def validate_pydantic_model(self, data: Any, model_class: type, 
                                    strict: bool = False) -> ValidationResult:
        """
        Validate data using a Pydantic model.
        
        Args:
            data: Data to validate
            model_class: Pydantic model class
            strict: Whether to use strict validation
            
        Returns:
            Validation result
        """
        result = ValidationResult()
        
        try:
            if isinstance(data, model_class):
                # Already a Pydantic model, validate it
                model_instance = data
            else:
                # Convert to Pydantic model
                if strict:
                    model_instance = model_class.model_validate(data, strict=True)
                else:
                    model_instance = model_class.model_validate(data)
            
            result.is_valid = True
            self.logger.debug(f"Pydantic validation successful for {model_class.__name__}")
            
        except PydanticValidationError as e:
            result.is_valid = False
            for error in e.errors():
                field = error.get('loc', ['unknown'])[0]
                message = error.get('msg', 'Validation error')
                result.add_error(f"{field}: {message}")
            
            self.logger.warning(f"Pydantic validation failed for {model_class.__name__}: {e}")
        
        except Exception as e:
            result.is_valid = False
            result.add_error(f"Validation error: {str(e)}")
            self.logger.error(f"Error during Pydantic validation: {str(e)}")
        
        return result
    
    async def validate_business_rules(self, data: Dict[str, Any], 
                                    domain: str = "general") -> ValidationResult:
        """
        Validate business rules for a specific domain.
        
        Args:
            data: Data to validate
            domain: Business domain (customer, editorial, company, security, compliance, audit, ml_models, data_quality, configuration)
            
        Returns:
            Validation result
        """
        result = ValidationResult()
        
        try:
            # Domain-specific business rule validation
            if domain == "customer":
                await self._validate_customer_business_rules(data, result)
            elif domain == "editorial":
                await self._validate_editorial_business_rules(data, result)
            elif domain == "company":
                await self._validate_company_business_rules(data, result)
            elif domain == "security":
                await self._validate_security_business_rules(data, result)
            elif domain == "compliance":
                await self._validate_compliance_business_rules(data, result)
            elif domain == "ml_models":
                await self._validate_ml_models_business_rules(data, result)
            else:
                # General business rules
                await self._validate_general_business_rules(data, result)
            
            self.logger.debug(f"Business rule validation completed for domain {domain}: {result.is_valid}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error during business rule validation: {str(e)}")
            result.add_error(f"Business rule validation error: {str(e)}")
            return result
    
    # ==================== DOMAIN-SPECIFIC VALIDATION METHODS ====================
    
    async def _validate_customer_business_rules(self, data: Dict[str, Any], result: ValidationResult) -> None:
        """Validate customer domain business rules."""
        # Session duration validation
        if 'duration_seconds' in data:
            duration = data['duration_seconds']
            if isinstance(duration, (int, float)) and duration < 0:
                result.add_error("Session duration must be non-negative")
        
        # Engagement score validation
        if 'engagement_score' in data:
            score = data['engagement_score']
            if isinstance(score, (int, float)) and (score < 0 or score > 1):
                result.add_error("Engagement score must be between 0 and 1")
    
    async def _validate_editorial_business_rules(self, data: Dict[str, Any], result: ValidationResult) -> None:
        """Validate editorial domain business rules."""
        # Article code format validation
        if 'article_code' in data:
            code = data['article_code']
            if code and not self._patterns['article_code'].match(code):
                result.add_error("Article code must be lowercase alphanumeric with hyphens")
        
        # Media code format validation
        if 'media_code' in data:
            code = data['media_code']
            if code and not self._patterns['media_code'].match(code):
                result.add_error("Media code must be lowercase alphanumeric with hyphens and underscores")
        
        # View count validation
        if 'view_count' in data:
            views = data['view_count']
            if isinstance(views, int) and views < 0:
                result.add_error("View count must be non-negative")
    
    async def _validate_company_business_rules(self, data: Dict[str, Any], result: ValidationResult) -> None:
        """Validate company domain business rules."""
        # Employee count validation
        if 'employee_count' in data:
            count = data['employee_count']
            if isinstance(count, int) and count < 0:
                result.add_error("Employee count must be non-negative")
        
        # Department level validation
        if 'department_level' in data:
            level = data['department_level']
            if isinstance(level, int) and (level < 1 or level > 10):
                result.add_error("Department level must be between 1 and 10")
    
    async def _validate_security_business_rules(self, data: Dict[str, Any], result: ValidationResult) -> None:
        """Validate security domain business rules."""
        # API key expiration validation
        if 'expires_at' in data and 'created_at' in data:
            expires = data['expires_at']
            created = data['created_at']
            if expires and created:
                if isinstance(expires, str):
                    from datetime import datetime
                    expires = datetime.fromisoformat(expires.replace('Z', '+00:00'))
                if isinstance(created, str):
                    from datetime import datetime
                    created = datetime.fromisoformat(created.replace('Z', '+00:00'))
                if expires < created:
                    result.add_error("API key expiration must be after creation date")
    
    async def _validate_compliance_business_rules(self, data: Dict[str, Any], result: ValidationResult) -> None:
        """Validate compliance domain business rules."""
        # Consent date validation
        if 'consent_date' in data and 'withdrawal_date' in data:
            consent = data['consent_date']
            withdrawal = data['withdrawal_date']
            if consent and withdrawal:
                if isinstance(withdrawal, str):
                    from datetime import datetime
                    withdrawal = datetime.fromisoformat(withdrawal.replace('Z', '+00:00'))
                if isinstance(consent, str):
                    from datetime import datetime
                    consent = datetime.fromisoformat(consent.replace('Z', '+00:00'))
                if withdrawal < consent:
                    result.add_error("Withdrawal date must be after consent date")
    
    async def _validate_ml_models_business_rules(self, data: Dict[str, Any], result: ValidationResult) -> None:
        """Validate ML models domain business rules."""
        # Model version validation
        if 'model_version' in data:
            version = data['model_version']
            if version and not isinstance(version, str):
                result.add_error("Model version must be a string")
        
        # Accuracy validation
        if 'accuracy' in data:
            accuracy = data['accuracy']
            if isinstance(accuracy, (int, float)) and (accuracy < 0 or accuracy > 1):
                result.add_error("Model accuracy must be between 0 and 1")
    
    async def _validate_general_business_rules(self, data: Dict[str, Any], result: ValidationResult) -> None:
        """Validate general business rules."""
        # Date consistency validation
        if 'created_at' in data and 'updated_at' in data:
            created = data['created_at']
            updated = data['updated_at']
            
            if isinstance(created, str):
                created = datetime.fromisoformat(created.replace('Z', '+00:00'))
            if isinstance(updated, str):
                updated = datetime.fromisoformat(updated.replace('Z', '+00:00'))
            
            if updated < created:
                result.add_error("Updated date cannot be before created date")
    
    # ==================== HELPER VALIDATION METHODS ====================
    
    def _validate_positive_number(self, value: Any) -> bool:
        """Validate positive number."""
        try:
            num = float(value)
            return num > 0
        except (ValueError, TypeError):
            return False
    
    # ==================== UTILITY METHODS ====================
    
    def sanitize_input(self, data: Any) -> Any:
        """
        Sanitize input data to prevent security issues.
        
        Args:
            data: Data to sanitize
            
        Returns:
            Sanitized data
        """
        if isinstance(data, str):
            # Remove script tags and their content
            data = re.sub(r'<script[^>]*>.*?</script>', '', data, flags=re.IGNORECASE | re.DOTALL)
            # Remove potentially dangerous characters
            data = re.sub(r'[<>"\']', '', data)
            # Remove common XSS patterns
            data = re.sub(r'javascript:', '', data, flags=re.IGNORECASE)
            data = re.sub(r'alert\s*\(', '', data, flags=re.IGNORECASE)
            # Trim whitespace
            data = data.strip()
        
        elif isinstance(data, dict):
            # Recursively sanitize dictionary values
            data = {k: self.sanitize_input(v) for k, v in data.items()}
        
        elif isinstance(data, list):
            # Recursively sanitize list items
            data = [self.sanitize_input(item) for item in data]
        
        return data
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """
        Get a summary of validation rules and patterns.
        
        Returns:
            Dictionary containing validation summary
        """
        return {
            "total_categories": len(self._rules),
            "total_rules": sum(len(rules) for rules in self._rules.values()),
            "available_patterns": list(self._patterns.keys()),
            "categories": list(self._rules.keys()),
            "timestamp": datetime.utcnow().isoformat()
        }

