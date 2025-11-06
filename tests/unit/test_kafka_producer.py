"""
Unit tests for Kafka producer
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime


class TestKafkaProducer:
    """Test Kafka producer functionality"""
    
    @patch('kafka_producer.KafkaProducer')
    def test_producer_creation(self, mock_producer_class):
        """Test Kafka producer initialization"""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        # Import and test the actual producer creation
        from kafka_producer import check_kafka_connection
        result = check_kafka_connection()
        
        # The function should attempt to create a producer
        assert mock_producer_class.called or result is False
    
    def test_event_structure(self):
        """Test event structure validation"""
        from kafka_producer import UserEvent
        
        # Create a test event with minimal required fields
        event = UserEvent(
            event_id="test-123",
            session_id="session-123",
            user_id=1,
            timestamp=datetime.now(),
            event_type="article_view",
            brand="test_brand",
            category="test_category",
            article_id="article-123",
            article_title="Test Article",
            article_type="news",
            page_url="https://example.com/article",
            referrer="direct",
            device_type="desktop",
            device_os="Windows",
            browser="Chrome",
            country="US",
            city="New York",
            timezone="America/New_York",
            subscription_tier="free",
            user_segment="casual",
            engagement_metrics={},
            metadata={}
        )
        
        assert event.event_id == "test-123"
        assert event.user_id == 1
        assert event.event_type == "article_view"
        assert event.brand == "test_brand"
    
    def test_session_management(self):
        """Test session management logic"""
        from kafka_producer import UserSession
        from datetime import datetime
        
        # Use a valid brand from BRANDS dictionary
        session = UserSession(
            user_id=1,
            brand="bild"  # Valid brand from BRANDS
        )
        
        assert session.user_id == 1
        assert session.brand == "bild"
        assert session.session_id is not None
        assert len(session.articles_viewed) == 0
        assert session.brand_config is not None
    
    def test_brand_configuration(self):
        """Test brand configuration"""
        from kafka_producer import BRANDS
        
        assert 'bild' in BRANDS
        assert 'welt' in BRANDS
        assert 'business_insider' in BRANDS
        assert 'politico' in BRANDS
        assert 'sport_bild' in BRANDS
        
        # Verify brand structure
        for brand_name, brand_config in BRANDS.items():
            assert 'categories' in brand_config
            assert 'article_types' in brand_config
            assert 'regions' in brand_config
    
    def test_event_types(self):
        """Test event types configuration"""
        from kafka_producer import EVENT_TYPES
        
        assert 'article_view' in EVENT_TYPES
        assert 'article_click' in EVENT_TYPES
        assert 'video_play' in EVENT_TYPES
        assert 'newsletter_signup' in EVENT_TYPES
        
        # Verify event type structure
        for event_type, event_config in EVENT_TYPES.items():
            assert 'weight' in event_config
            assert 'follow_up_events' in event_config

