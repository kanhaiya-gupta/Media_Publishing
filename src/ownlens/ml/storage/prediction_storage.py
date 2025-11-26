"""
OwnLens - ML Module: Prediction Storage

Prediction storage client for saving predictions to ClickHouse.
"""

from typing import Any, Dict, Optional, Union
from datetime import date, datetime
import uuid
import json
import logging
import pandas as pd
import numpy as np
from clickhouse_driver import Client

from ownlens.ml.utils.config import get_ml_config


logger = logging.getLogger(__name__)


class PredictionStorage:
    """
    Prediction storage client for saving predictions to ClickHouse.
    
    Handles:
    - Saving predictions to ml_model_predictions (unified)
    - Saving to domain-specific tables
    - Tracking prediction history
    """
    
    def __init__(self, client: Optional[Client] = None):
        """
        Initialize prediction storage.
        
        Args:
            client: ClickHouse client (if None, creates new)
        """
        self.config = get_ml_config()
        self.client = client
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info("Initialized prediction storage")
    
    def _get_client(self) -> Client:
        """Get ClickHouse client."""
        if self.client:
            return self.client
        return Client(**self.config.get_clickhouse_connection_params())
    
    def save_prediction(
        self,
        model_id: str,
        entity_id: str,
        entity_type: str,
        prediction_type: str,
        prediction_value: Optional[float] = None,
        prediction_probability: Optional[float] = None,
        prediction_class: Optional[str] = None,
        prediction_confidence: Optional[float] = None,
        input_features: Optional[Dict[str, Any]] = None,
        company_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        batch_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        """
        Save prediction to ml_model_predictions (unified table).
        
        Args:
            model_id: Model ID
            entity_id: Entity ID (user_id, article_id, etc.)
            entity_type: Entity type ('user', 'article', 'content')
            prediction_type: Prediction type ('churn', 'conversion', 'article_performance', etc.)
            prediction_value: Prediction value (for regression)
            prediction_probability: Prediction probability (for classification)
            prediction_class: Prediction class (for classification)
            prediction_confidence: Prediction confidence score
            input_features: Input features used (JSON)
            company_id: Company ID
            brand_id: Brand ID
            batch_id: Batch ID (for batch predictions)
            metadata: Additional metadata
            **kwargs: Additional parameters
        
        Returns:
            prediction_id (UUID string)
        """
        client = self._get_client()
        
        try:
            # Generate prediction_id
            prediction_id = str(uuid.uuid4())
            
            # Prepare JSON fields
            input_features_json = json.dumps(input_features) if input_features else ''
            metadata_json = json.dumps(metadata) if metadata else ''
            
            # Insert into ml_model_predictions
            # Format values directly - ClickHouse doesn't support parameterized INSERT queries well
            def escape_sql(value):
                if value is None:
                    return 'NULL'
                if isinstance(value, str):
                    escaped = value.replace("'", "''")
                    return f"'{escaped}'"
                if isinstance(value, (int, float)):
                    return str(value)
                return str(value)
            
            prediction_id_str = escape_sql(prediction_id)
            model_id_str = escape_sql(model_id)
            prediction_type_str = escape_sql(prediction_type)
            entity_id_str = escape_sql(entity_id)
            entity_type_str = escape_sql(entity_type)
            prediction_value_str = escape_sql(prediction_value) if prediction_value is not None else 'NULL'
            prediction_probability_str = escape_sql(prediction_probability) if prediction_probability is not None else 'NULL'
            prediction_class_str = escape_sql(prediction_class) if prediction_class is not None else 'NULL'
            prediction_confidence_str = escape_sql(prediction_confidence) if prediction_confidence is not None else 'NULL'
            input_features_str = escape_sql(input_features_json) if input_features_json else "''"
            company_id_str = escape_sql(company_id) if company_id else 'NULL'
            brand_id_str = escape_sql(brand_id) if brand_id else 'NULL'
            batch_id_str = escape_sql(batch_id) if batch_id else 'NULL'
            metadata_str = escape_sql(metadata_json) if metadata_json else "''"
            
            query = f"""
            INSERT INTO ml_model_predictions (
                prediction_id, model_id,
                prediction_type, entity_id, entity_type,
                prediction_value, prediction_probability, prediction_class, prediction_confidence,
                input_features, actual_value, actual_class, is_correct,
                predicted_at, prediction_date, actual_observed_at,
                company_id, brand_id, batch_id,
                metadata, created_at
            ) VALUES (
                {prediction_id_str}, {model_id_str},
                {prediction_type_str}, {entity_id_str}, {entity_type_str},
                {prediction_value_str}, {prediction_probability_str}, {prediction_class_str}, {prediction_confidence_str},
                {input_features_str}, NULL, NULL, NULL,
                now(), today(), NULL,
                {company_id_str}, {brand_id_str}, {batch_id_str},
                {metadata_str}, now()
            )
            """
            
            client.execute(query)
            
            self.logger.info(f"Saved prediction: {prediction_id} ({prediction_type} for {entity_type} {entity_id})")
            return prediction_id
            
        finally:
            if not self.client:
                client.disconnect()
    
    def save_churn_prediction(
        self,
        user_id: str,
        prediction: Union[pd.DataFrame, Dict[str, Any]],
        model_id: str,
        company_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        features: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        """
        Save churn prediction to customer_churn_predictions.
        
        Args:
            user_id: User ID
            prediction: Prediction DataFrame or dict with churn_probability
            model_id: Model ID
            company_id: Company ID
            brand_id: Brand ID
            features: Features used for prediction
            **kwargs: Additional parameters
        
        Returns:
            prediction_id
        """
        client = self._get_client()
        
        try:
            # Extract prediction data
            if isinstance(prediction, pd.DataFrame):
                churn_probability = float(prediction['churn_probability'].iloc[0]) if 'churn_probability' in prediction.columns else float(prediction.iloc[0, 0])
            else:
                churn_probability = float(prediction.get('churn_probability', prediction.get('prob_churned', 0.0)))
            
            # Calculate churn risk level
            if churn_probability < 0.3:
                churn_risk_level = 'low'
            elif churn_probability < 0.6:
                churn_risk_level = 'medium'
            elif churn_probability < 0.8:
                churn_risk_level = 'high'
            else:
                churn_risk_level = 'critical'
            
            # Extract features if available
            days_since_last_session = features.get('days_since_last_session', 0) if features else 0
            total_sessions = features.get('total_sessions', 0) if features else 0
            avg_engagement_score = features.get('avg_engagement_score', 0.0) if features else 0.0
            subscription_tier = features.get('subscription_tier', '') if features else ''
            
            # Generate prediction_id
            prediction_id = str(uuid.uuid4())
            
            # Get model version from registry
            model_version = kwargs.get('model_version', '1.0.0')
            model_confidence = kwargs.get('model_confidence', churn_probability)
            feature_importance = json.dumps(features) if features else ''
            
            # Insert into customer_churn_predictions
            query = """
            INSERT INTO customer_churn_predictions (
                prediction_id, user_id, company_id, brand_id, prediction_date,
                churn_probability, churn_risk_level, is_churned, churned_date,
                model_version, model_confidence, feature_importance,
                days_since_last_session, total_sessions, avg_engagement_score, subscription_tier,
                created_at
            ) VALUES (
                ?, ?, ?, ?, today(),
                ?, ?, 0, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                now()
            )
            """
            
            params = [
                prediction_id, user_id, company_id, brand_id,
                churn_probability, churn_risk_level, None,  # churned_date filled later
                model_version, model_confidence, feature_importance,
                int(days_since_last_session), int(total_sessions), float(avg_engagement_score), subscription_tier
            ]
            
            client.execute(query, params)
            
            # Also save to unified table
            self.save_prediction(
                model_id=model_id,
                entity_id=user_id,
                entity_type='user',
                prediction_type='churn',
                prediction_probability=churn_probability,
                prediction_class=churn_risk_level,
                prediction_confidence=model_confidence,
                input_features=features,
                company_id=company_id,
                brand_id=brand_id,
                metadata={'churn_risk_level': churn_risk_level}
            )
            
            self.logger.info(f"Saved churn prediction: {prediction_id} for user {user_id} (risk: {churn_risk_level})")
            return prediction_id
            
        finally:
            if not self.client:
                client.disconnect()
    
    def save_conversion_prediction(
        self,
        user_id: str,
        prediction: Union[pd.DataFrame, Dict[str, Any]],
        model_id: str,
        company_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        features: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        """
        Save conversion prediction to customer_conversion_predictions.
        
        Args:
            user_id: User ID
            prediction: Prediction DataFrame or dict with conversion_probability
            model_id: Model ID
            company_id: Company ID
            brand_id: Brand ID
            features: Features used for prediction
            **kwargs: Additional parameters
        
        Returns:
            prediction_id
        """
        client = self._get_client()
        
        try:
            # Extract prediction data
            if isinstance(prediction, pd.DataFrame):
                conversion_probability = float(prediction['conversion_probability'].iloc[0]) if 'conversion_probability' in prediction.columns else float(prediction.iloc[0, 0])
                conversion_tier = str(prediction['conversion_tier'].iloc[0]) if 'conversion_tier' in prediction.columns else 'medium'
            else:
                conversion_probability = float(prediction.get('conversion_probability', prediction.get('prob_converted', 0.0)))
                conversion_tier = str(prediction.get('conversion_tier', 'medium'))
            
            # Calculate conversion value (optional)
            conversion_value = kwargs.get('conversion_value', conversion_probability * 100.0)
            
            # Generate prediction_id
            prediction_id = str(uuid.uuid4())
            
            # Get model version from registry
            model_version = kwargs.get('model_version', '1.0.0')
            model_confidence = kwargs.get('model_confidence', conversion_probability)
            feature_importance = json.dumps(features) if features else ''
            
            # Insert into customer_conversion_predictions
            query = """
            INSERT INTO customer_conversion_predictions (
                prediction_id, user_id, company_id, brand_id, prediction_date,
                conversion_probability, conversion_tier, conversion_value,
                model_version, model_confidence, feature_importance,
                did_convert, converted_at, actual_tier,
                created_at
            ) VALUES (
                ?, ?, ?, ?, today(),
                ?, ?, ?,
                ?, ?, ?,
                0, ?, ?,
                now()
            )
            """
            
            params = [
                prediction_id, user_id, company_id, brand_id,
                conversion_probability, conversion_tier, conversion_value,
                model_version, model_confidence, feature_importance,
                None, None  # converted_at, actual_tier filled later
            ]
            
            client.execute(query, params)
            
            # Also save to unified table
            self.save_prediction(
                model_id=model_id,
                entity_id=user_id,
                entity_type='user',
                prediction_type='conversion',
                prediction_probability=conversion_probability,
                prediction_class=conversion_tier,
                prediction_confidence=model_confidence,
                input_features=features,
                company_id=company_id,
                brand_id=brand_id,
                metadata={'conversion_tier': conversion_tier}
            )
            
            self.logger.info(f"Saved conversion prediction: {prediction_id} for user {user_id} (tier: {conversion_tier})")
            return prediction_id
            
        finally:
            if not self.client:
                client.disconnect()
    
    def save_recommendation(
        self,
        user_id: str,
        article_id: str,
        score: float,
        rank: int,
        model_id: str,
        company_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        category_id: Optional[str] = None,
        recommendation_type: str = 'collaborative',
        algorithm: str = 'NMF',
        **kwargs
    ) -> str:
        """
        Save recommendation to customer_recommendations.
        
        Args:
            user_id: User ID
            article_id: Article ID
            score: Recommendation score
            rank: Rank position
            model_id: Model ID
            company_id: Company ID
            brand_id: Brand ID
            category_id: Category ID
            recommendation_type: Recommendation type ('collaborative', 'content_based', 'hybrid')
            algorithm: Algorithm used ('NMF', 'content_based', 'hybrid')
            **kwargs: Additional parameters
        
        Returns:
            recommendation_id
        """
        client = self._get_client()
        
        try:
            # Generate recommendation_id
            recommendation_id = str(uuid.uuid4())
            
            # Get model version
            model_version = kwargs.get('model_version', '1.0.0')
            expires_at = kwargs.get('expires_at', datetime.now().replace(day=1, month=1, year=datetime.now().year + 1))
            
            # Insert into customer_recommendations
            # Format values directly - ClickHouse doesn't support parameterized INSERT queries well
            def escape_sql(value):
                if value is None:
                    return 'NULL'
                if isinstance(value, str):
                    escaped = value.replace("'", "''")
                    return f"'{escaped}'"
                return str(value)
            
            recommendation_id_str = escape_sql(recommendation_id)
            user_id_str = escape_sql(user_id)
            company_id_str = escape_sql(company_id) if company_id else 'NULL'
            brand_id_str = escape_sql(brand_id) if brand_id else 'NULL'
            article_id_str = escape_sql(article_id)
            category_id_str = escape_sql(category_id) if category_id else 'NULL'
            recommendation_type_str = escape_sql(recommendation_type)
            score_str = str(float(score))
            rank_str = str(int(rank))
            model_version_str = escape_sql(model_version)
            algorithm_str = escape_sql(algorithm)
            expires_at_str = escape_sql(expires_at.strftime('%Y-%m-%d')) if isinstance(expires_at, datetime) else escape_sql(expires_at)
            
            query = f"""
            INSERT INTO customer_recommendations (
                recommendation_id, user_id, company_id, brand_id, article_id, category_id,
                recommendation_type, recommendation_score, rank_position,
                model_version, algorithm,
                was_shown, was_clicked, was_viewed, clicked_at, viewed_at,
                created_at, expires_at
            ) VALUES (
                {recommendation_id_str}, {user_id_str}, {company_id_str}, {brand_id_str}, {article_id_str}, {category_id_str},
                {recommendation_type_str}, {score_str}, {rank_str},
                {model_version_str}, {algorithm_str},
                0, 0, 0, NULL, NULL,
                now(), {expires_at_str}
            )
            """
            
            client.execute(query)
            
            # Also save to unified ml_model_predictions table for monitoring
            self.save_prediction(
                model_id=model_id,
                entity_id=user_id,
                entity_type='user',
                prediction_type='recommendation',
                prediction_value=float(score),
                prediction_confidence=float(score),  # Use score as confidence
                company_id=company_id,
                brand_id=brand_id,
                metadata={
                    'article_id': article_id,
                    'rank': rank,
                    'recommendation_type': recommendation_type,
                    'algorithm': algorithm
                }
            )
            
            self.logger.info(f"Saved recommendation: {recommendation_id} for user {user_id} (article {article_id}, rank {rank})")
            return recommendation_id
            
        finally:
            if not self.client:
                client.disconnect()
    
    def save_segment_assignment(
        self,
        user_id: str,
        segment_id: str,
        confidence: float,
        model_id: str,
        company_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Save segment assignment to customer_user_segment_assignments.
        
        Args:
            user_id: User ID
            segment_id: Segment ID
            confidence: Confidence score
            model_id: Model ID
            company_id: Company ID
            brand_id: Brand ID
            **kwargs: Additional parameters
        
        Returns:
            assignment_id
        """
        client = self._get_client()
        
        try:
            # Generate assignment_id
            assignment_id = str(uuid.uuid4())
            
            # Get model version
            model_version = kwargs.get('model_version', '1.0.0')
            
            # Note: customer_user_segment_assignments is a ReplacingMergeTree table
            # We just insert a new row with is_current=1 - the table will handle deduplication
            # based on the primary key (user_id, company_id, brand_id, assignment_date)
            # The latest row with is_current=1 will be kept by ReplacingMergeTree
            
            # Insert new assignment
            # Format values directly - ClickHouse doesn't support parameterized INSERT queries well
            def escape_sql(value):
                if value is None:
                    return 'NULL'
                if isinstance(value, str):
                    escaped = value.replace("'", "''")
                    return f"'{escaped}'"
                return str(value)
            
            assignment_id_str = escape_sql(assignment_id)
            user_id_str = escape_sql(user_id)
            company_id_str = escape_sql(company_id) if company_id else 'NULL'
            brand_id_str = escape_sql(brand_id) if brand_id else 'NULL'
            segment_id_str = escape_sql(segment_id)
            confidence_str = str(float(confidence))
            model_version_str = escape_sql(model_version)
            
            query = f"""
            INSERT INTO customer_user_segment_assignments (
                assignment_id, user_id, company_id, brand_id, segment_id,
                assignment_date, confidence_score, model_version, is_current,
                created_at
            ) VALUES (
                {assignment_id_str}, {user_id_str}, {company_id_str}, {brand_id_str}, {segment_id_str},
                today(), {confidence_str}, {model_version_str}, 1,
                now()
            )
            """
            
            client.execute(query)
            
            # Also save to unified ml_model_predictions table for monitoring
            self.save_prediction(
                model_id=model_id,
                entity_id=user_id,
                entity_type='user',
                prediction_type='segmentation',
                prediction_class=segment_id,
                prediction_confidence=confidence,
                company_id=company_id,
                brand_id=brand_id,
                metadata={'segment_id': segment_id, 'confidence': confidence}
            )
            
            self.logger.info(f"Saved segment assignment: {assignment_id} for user {user_id} (segment {segment_id})")
            return assignment_id
            
        finally:
            if not self.client:
                client.disconnect()
    
    def update_prediction_actual(
        self,
        prediction_id: str,
        actual_value: Optional[float] = None,
        actual_class: Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Update prediction with actual outcome (for validation).
        
        Args:
            prediction_id: Prediction ID
            actual_value: Actual value (for regression)
            actual_class: Actual class (for classification)
            **kwargs: Additional parameters
        
        Returns:
            True if successful
        """
        client = self._get_client()
        
        try:
            # Format values directly - ClickHouse doesn't support parameterized queries well
            def escape_sql(value):
                if value is None:
                    return 'NULL'
                if isinstance(value, str):
                    escaped = value.replace("'", "''")
                    return f"'{escaped}'"
                return str(value)
            
            # Calculate is_correct if we have both prediction and actual
            is_correct = None
            if actual_class is not None:
                # Get prediction class
                prediction_id_str = escape_sql(prediction_id)
                query = f"SELECT prediction_class FROM ml_model_predictions WHERE prediction_id = {prediction_id_str}"
                results = client.execute(query)
                if results:
                    prediction_class = results[0][0]
                    is_correct = 1 if prediction_class == actual_class else 0
            
            # Update ml_model_predictions
            prediction_id_str = escape_sql(prediction_id)
            actual_value_str = escape_sql(actual_value) if actual_value is not None else 'NULL'
            actual_class_str = escape_sql(actual_class) if actual_class is not None else 'NULL'
            is_correct_str = escape_sql(is_correct) if is_correct is not None else 'NULL'
            
            query = f"""
            ALTER TABLE ml_model_predictions
            UPDATE 
                actual_value = {actual_value_str},
                actual_class = {actual_class_str},
                is_correct = {is_correct_str},
                actual_observed_at = now(),
                updated_at = now()
            WHERE prediction_id = {prediction_id_str}
            """
            
            client.execute(query)
            
            self.logger.info(f"Updated prediction {prediction_id} with actual outcome")
            return True
            
        finally:
            if not self.client:
                client.disconnect()

