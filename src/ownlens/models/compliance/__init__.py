"""
OwnLens - Compliance Domain Models

Compliance and data privacy: GDPR, consent management, data retention.
"""

from .user_consent import UserConsent, UserConsentCreate, UserConsentUpdate
from .data_subject_request import DataSubjectRequest, DataSubjectRequestCreate, DataSubjectRequestUpdate
from .retention_policy import RetentionPolicy, RetentionPolicyCreate, RetentionPolicyUpdate
from .retention_execution import RetentionExecution, RetentionExecutionCreate, RetentionExecutionUpdate
from .anonymized_data import AnonymizedData, AnonymizedDataCreate, AnonymizedDataUpdate
from .privacy_assessment import PrivacyAssessment, PrivacyAssessmentCreate, PrivacyAssessmentUpdate
from .breach_incident import BreachIncident, BreachIncidentCreate, BreachIncidentUpdate

__all__ = [
    "UserConsent",
    "UserConsentCreate",
    "UserConsentUpdate",
    "DataSubjectRequest",
    "DataSubjectRequestCreate",
    "DataSubjectRequestUpdate",
    "RetentionPolicy",
    "RetentionPolicyCreate",
    "RetentionPolicyUpdate",
    "RetentionExecution",
    "RetentionExecutionCreate",
    "RetentionExecutionUpdate",
    "AnonymizedData",
    "AnonymizedDataCreate",
    "AnonymizedDataUpdate",
    "PrivacyAssessment",
    "PrivacyAssessmentCreate",
    "PrivacyAssessmentUpdate",
    "BreachIncident",
    "BreachIncidentCreate",
    "BreachIncidentUpdate",
]

