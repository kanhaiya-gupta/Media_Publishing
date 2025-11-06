#!/usr/bin/env python3
"""
Media Publishing - Real-Time User Analytics Event Producer

This producer simulates real-world user interactions across digital media properties
digital media properties (Bild, Die Welt, Business Insider, Politico, etc.)

Events tracked:
- Article views and engagement
- Time spent reading
- User navigation patterns
- Subscription events
- Video consumption
- Newsletter interactions
- Geographic and device analytics

Author: Data Engineering Team
Company: Media Publishing
"""

import json
import logging
import random
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "web_clicks"
EVENT_RATE_SECONDS = 0.1  # Simulate 10 events per second
MAX_SESSION_DURATION_SECONDS = 300  # 5 minutes max session
MIN_SESSION_DURATION_SECONDS = 10  # 10 seconds min session

# Media Publishing Brand Portfolio
BRANDS = {
    "bild": {
        "categories": ["politics", "sports", "entertainment", "lifestyle", "crime"],
        "article_types": ["news", "opinion", "gallery", "video"],
        "regions": ["DE", "AT", "CH"],
    },
    "welt": {
        "categories": ["politics", "business", "technology", "culture", "opinion"],
        "article_types": ["news", "analysis", "feature", "interview"],
        "regions": ["DE", "AT", "CH", "US"],
    },
    "business_insider": {
        "categories": ["business", "tech", "finance", "careers", "markets"],
        "article_types": ["news", "analysis", "feature", "listicle"],
        "regions": ["US", "UK", "DE", "FR", "AU"],
    },
    "politico": {
        "categories": ["politics", "policy", "elections", "congress", "white-house"],
        "article_types": ["news", "analysis", "briefing", "live-blog"],
        "regions": ["US", "EU"],
    },
    "sport_bild": {
        "categories": ["football", "basketball", "tennis", "olympics", "transfer"],
        "article_types": ["news", "match-report", "interview", "analysis"],
        "regions": ["DE", "AT", "CH", "EU"],
    },
}

# User Engagement Events
EVENT_TYPES = {
    "article_view": {
        "weight": 0.6,
        "follow_up_events": ["scroll", "time_on_page", "article_complete"],
    },
    "article_click": {"weight": 0.15, "follow_up_events": ["article_view"]},
    "video_play": {
        "weight": 0.05,
        "follow_up_events": ["video_complete", "video_pause"],
    },
    "newsletter_signup": {"weight": 0.02, "follow_up_events": []},
    "subscription_prompt": {
        "weight": 0.03,
        "follow_up_events": ["subscription_conversion"],
    },
    "ad_click": {"weight": 0.05, "follow_up_events": []},
    "search": {"weight": 0.05, "follow_up_events": ["article_view"]},
    "navigation": {"weight": 0.05, "follow_up_events": ["article_view"]},
}

# Device Types
DEVICES = ["desktop", "mobile", "tablet", "smart_tv", "app"]
MOBILE_OS = ["iOS", "Android"]
DESKTOP_OS = ["Windows", "macOS", "Linux"]

# Subscription Tiers
SUBSCRIPTION_TIERS = ["free", "premium", "pro", "enterprise"]


@dataclass
class UserEvent:
    """Structured event schema for user interactions"""

    event_id: str
    user_id: int
    session_id: str
    event_type: str
    timestamp: int
    brand: str
    category: str
    article_id: str
    article_title: str
    article_type: str
    page_url: str
    referrer: str
    device_type: str
    device_os: str
    browser: str
    country: str
    city: str
    timezone: str
    subscription_tier: str
    user_segment: str
    engagement_metrics: Dict[str, Any]
    metadata: Dict[str, Any]


class UserSession:
    """Represents a user session with realistic behavior"""

    def __init__(self, user_id: int, brand: str):
        self.user_id = user_id
        self.session_id = str(uuid.uuid4())
        self.brand = brand
        self.brand_config = BRANDS[brand]
        self.start_time = int(time.time())
        self.current_time = self.start_time
        self.subscription_tier = random.choice(SUBSCRIPTION_TIERS)
        self.device_type = random.choice(DEVICES)
        self.device_os = self._get_device_os()
        self.browser = random.choice(["Chrome", "Safari", "Firefox", "Edge", "Opera"])
        self.country = random.choice(self.brand_config["regions"])
        self.city = self._get_city_for_country(self.country)
        self.timezone = self._get_timezone(self.country)
        self.user_segment = self._get_user_segment()
        self.articles_viewed = []
        self.session_duration = random.randint(MIN_SESSION_DURATION_SECONDS, MAX_SESSION_DURATION_SECONDS)
        self.current_page = None
        self.referrer = random.choice(
            [
                "direct",
                "google",
                "facebook",
                "twitter",
                "newsletter",
                "internal",
                "reddit",
                "linkedin",
                "bing",
            ]
        )

    def _get_device_os(self) -> str:
        if self.device_type == "mobile":
            return random.choice(MOBILE_OS)
        elif self.device_type in ["desktop", "tablet"]:
            return random.choice(DESKTOP_OS)
        return "Unknown"

    def _get_city_for_country(self, country: str) -> str:
        cities = {
            "DE": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"],
            "US": [
                "New York",
                "Washington DC",
                "Los Angeles",
                "Chicago",
                "San Francisco",
            ],
            "UK": ["London", "Manchester", "Birmingham", "Edinburgh"],
            "FR": ["Paris", "Lyon", "Marseille", "Toulouse"],
            "AT": ["Vienna", "Graz", "Salzburg"],
            "CH": ["Zurich", "Geneva", "Bern"],
            "EU": ["Brussels", "Strasbourg", "Luxembourg"],
            "AU": ["Sydney", "Melbourne", "Canberra"],
        }
        return random.choice(cities.get(country, ["Unknown"]))

    def _get_timezone(self, country: str) -> str:
        timezones = {
            "DE": "Europe/Berlin",
            "AT": "Europe/Vienna",
            "CH": "Europe/Zurich",
            "US": "America/New_York",
            "UK": "Europe/London",
            "FR": "Europe/Paris",
            "AU": "Australia/Sydney",
            "EU": "Europe/Brussels",
        }
        return timezones.get(country, "UTC")

    def _get_user_segment(self) -> str:
        """Segment users based on subscription and behavior"""
        if self.subscription_tier != "free":
            return random.choice(["power_user", "engaged", "subscriber"])
        return random.choice(["casual", "new_visitor", "returning"])

    def generate_article_id(self) -> str:
        """Generate realistic article ID"""
        return f"{self.brand}_{random.randint(1000, 9999)}_{int(time.time())}"

    def generate_article_title(self, category: str) -> str:
        """Generate realistic article titles by category"""
        titles = {
            "politics": [
                "Breaking: Major Policy Shift Announced",
                "Election Results: Key Races to Watch",
                "Analysis: What This Means for Voters",
                "Exclusive: Interview with Key Decision Maker",
            ],
            "sports": [
                "Match Report: Dramatic Win",
                "Transfer News: Star Player Moves",
                "Championship Preview: Everything You Need to Know",
                "Post-Match Analysis: Key Takeaways",
            ],
            "business": [
                "Market Update: Stocks Surge",
                "CEO Interview: Company Strategy Revealed",
                "Economic Impact: What Experts Say",
                "Tech IPO: Breaking Records",
            ],
            "tech": [
                "New AI Breakthrough Announced",
                "Product Launch: What to Expect",
                "Security Alert: Important Update",
                "Innovation Spotlight: Emerging Technology",
            ],
            "entertainment": [
                "Exclusive: Celebrity Interview",
                "Award Show Recap: Winners and Highlights",
                "Movie Review: Critics Weigh In",
                "TV Series Premiere: First Impressions",
            ],
        }
        return random.choice(titles.get(category, ["News Article"]))

    def generate_event(self) -> UserEvent:
        """Generate a realistic user event"""
        # Select event type based on weights
        event_type = self._weighted_choice(EVENT_TYPES)
        event_config = EVENT_TYPES[event_type]

        # Generate article metadata if article-related event
        if event_type in ["article_view", "article_click"]:
            category = random.choice(self.brand_config["categories"])
            article_type = random.choice(self.brand_config["article_types"])
            article_id = self.generate_article_id()
            article_title = self.generate_article_title(category)
            page_url = f"https://{self.brand}.com/{category}/{article_id}"

            if article_id not in self.articles_viewed:
                self.articles_viewed.append(article_id)
                self.current_page = article_id
        else:
            category = random.choice(self.brand_config["categories"])
            article_type = None
            article_id = self.current_page or self.generate_article_id()
            article_title = self.generate_article_title(category)
            page_url = f"https://{self.brand}.com/{category}"

        # Generate engagement metrics
        engagement_metrics = self._generate_engagement_metrics(event_type)

        # Generate metadata
        metadata = self._generate_metadata(event_type)

        # Update timestamp
        self.current_time += random.randint(1, 30)  # Events 1-30 seconds apart

        return UserEvent(
            event_id=str(uuid.uuid4()),
            user_id=self.user_id,
            session_id=self.session_id,
            event_type=event_type,
            timestamp=self.current_time,
            brand=self.brand,
            category=category,
            article_id=article_id,
            article_title=article_title,
            article_type=article_type or "unknown",
            page_url=page_url,
            referrer=self.referrer if len(self.articles_viewed) == 1 else "internal",
            device_type=self.device_type,
            device_os=self.device_os,
            browser=self.browser,
            country=self.country,
            city=self.city,
            timezone=self.timezone,
            subscription_tier=self.subscription_tier,
            user_segment=self.user_segment,
            engagement_metrics=engagement_metrics,
            metadata=metadata,
        )

    def _weighted_choice(self, choices: Dict) -> str:
        """Select event type based on weights"""
        total_weight = sum(config["weight"] for config in choices.values())
        rand = random.uniform(0, total_weight)
        cumulative = 0
        for event_type, config in choices.items():
            cumulative += config["weight"]
            if rand <= cumulative:
                return event_type
        return list(choices.keys())[0]

    def _generate_engagement_metrics(self, event_type: str) -> Dict[str, Any]:
        """Generate realistic engagement metrics"""
        metrics = {}

        if event_type == "article_view":
            metrics["scroll_depth"] = random.randint(0, 100)  # Percentage
            metrics["time_on_page"] = random.randint(5, 600)  # Seconds
            metrics["read_progress"] = random.randint(0, 100)
            metrics["is_article_complete"] = random.choice([True, False])

        elif event_type == "video_play":
            metrics["video_duration"] = random.randint(30, 600)  # Seconds
            metrics["playback_position"] = random.randint(0, metrics["video_duration"])
            metrics["video_completed"] = random.choice([True, False])
            metrics["playback_rate"] = random.choice([0.5, 1.0, 1.25, 1.5, 2.0])

        elif event_type == "search":
            metrics["search_query"] = random.choice(
                [
                    "politics",
                    "sports news",
                    "business update",
                    "tech review",
                    "weather",
                    "stock market",
                    "election results",
                ]
            )
            metrics["results_count"] = random.randint(10, 1000)
            metrics["clicked_result"] = random.choice([True, False])

        elif event_type == "ad_click":
            metrics["ad_position"] = random.choice(["header", "sidebar", "in-article", "footer"])
            metrics["ad_category"] = random.choice(["automotive", "finance", "tech", "retail"])
            metrics["ad_revenue"] = round(random.uniform(0.1, 5.0), 2)

        return metrics

    def _generate_metadata(self, event_type: str) -> Dict[str, Any]:
        """Generate additional metadata"""
        metadata = {
            "session_start_time": self.start_time,
            "session_duration": self.current_time - self.start_time,
            "articles_in_session": len(self.articles_viewed),
            "is_new_session": len(self.articles_viewed) == 1,
            "user_agent": f"{self.browser}/{random.randint(90, 120)}.0",
            "screen_resolution": self._get_screen_resolution(),
            "language": self._get_language(self.country),
            "is_mobile": self.device_type == "mobile",
            "is_tablet": self.device_type == "tablet",
            "is_desktop": self.device_type == "desktop",
        }

        if event_type == "newsletter_signup":
            metadata["newsletter_type"] = random.choice(["daily_digest", "breaking_news", "weekly_summary", "topic_specific"])
            metadata["email_provided"] = True

        if event_type == "subscription_prompt":
            metadata["prompt_type"] = random.choice(["modal", "banner", "inline"])
            metadata["articles_viewed_before_prompt"] = len(self.articles_viewed)

        return metadata

    def _get_screen_resolution(self) -> str:
        """Get realistic screen resolution based on device"""
        if self.device_type == "mobile":
            return random.choice(["375x667", "414x896", "390x844", "360x640"])
        elif self.device_type == "tablet":
            return random.choice(["768x1024", "810x1080", "834x1194"])
        else:  # desktop
            return random.choice(["1920x1080", "1366x768", "1440x900", "2560x1440"])

    def _get_language(self, country: str) -> str:
        """Get language based on country"""
        languages = {
            "DE": "de-DE",
            "AT": "de-AT",
            "CH": "de-CH",
            "US": "en-US",
            "UK": "en-GB",
            "FR": "fr-FR",
            "AU": "en-AU",
            "EU": "en-GB",
        }
        return languages.get(country, "en-US")

    def is_session_active(self) -> bool:
        """Check if session is still active"""
        return (self.current_time - self.start_time) < self.session_duration

    def generate_session_end_event(self) -> UserEvent:
        """Generate session end event"""
        return UserEvent(
            event_id=str(uuid.uuid4()),
            user_id=self.user_id,
            session_id=self.session_id,
            event_type="session_end",
            timestamp=self.current_time,
            brand=self.brand,
            category="",
            article_id="",
            article_title="",
            article_type="",
            page_url="",
            referrer=self.referrer,
            device_type=self.device_type,
            device_os=self.device_os,
            browser=self.browser,
            country=self.country,
            city=self.city,
            timezone=self.timezone,
            subscription_tier=self.subscription_tier,
            user_segment=self.user_segment,
            engagement_metrics={
                "total_session_duration": self.current_time - self.start_time,
                "total_articles_viewed": len(self.articles_viewed),
                "total_events": len(self.articles_viewed) + 1,
            },
            metadata={
                "session_summary": {
                    "start_time": self.start_time,
                    "end_time": self.current_time,
                    "duration_seconds": self.current_time - self.start_time,
                    "articles_viewed": self.articles_viewed,
                    "brand": self.brand,
                    "exit_page": self.current_page or "unknown",
                }
            },
        )


def check_kafka_connection(bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS) -> bool:
    """Check if Kafka broker is available"""
    try:
        test_producer = KafkaProducer(bootstrap_servers=bootstrap_servers, request_timeout_ms=3000)
        test_producer.close(timeout=1)
        logger.info(f"✓ Successfully connected to Kafka at {bootstrap_servers}")
        return True
    except NoBrokersAvailable:
        logger.error(f"✗ Cannot connect to Kafka broker at {bootstrap_servers}")
        print("\nPlease ensure Kafka is running:")
        print("  Option 1 - Docker (Recommended):")
        print("    cd backend/kafka_check")
        print("    docker-compose up -d")
        print("\n  Option 2 - Manual:")
        print("    1. Start Zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties")
        print("    2. Start Kafka: bin/kafka-server-start.sh config/server.properties")
        return False
    except Exception as e:
        logger.error(f"✗ Error connecting to Kafka: {e}")
        return False


def main():
    """Main producer loop"""
    print("=" * 80)
    print("Media Publishing - Real-Time User Analytics Event Producer")
    print("=" * 80)
    print(f"Brands: {', '.join(BRANDS.keys())}")
    print(f"Event Types: {', '.join(EVENT_TYPES.keys())}")
    print(f"Target: {KAFKA_TOPIC} @ {KAFKA_BOOTSTRAP_SERVERS}")
    print("=" * 80)

    # Check Kafka connection
    if not check_kafka_connection():
        sys.exit(1)

    # Initialize Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            # Production settings
            acks="all",  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,
            enable_idempotence=True,
            compression_type="snappy",
        )
        logger.info("✓ Kafka producer initialized successfully\n")
    except Exception as e:
        logger.error(f"✗ Failed to create Kafka producer: {e}")
        sys.exit(1)

    # Statistics
    total_events = 0
    total_sessions = 0
    events_by_brand = {brand: 0 for brand in BRANDS.keys()}
    start_time = time.time()

    try:
        user_id = 1
        while True:
            # Select random brand for each user
            brand = random.choice(list(BRANDS.keys()))
            session = UserSession(user_id, brand)

            logger.info(f"\n📱 Starting session {session.session_id} for user {user_id} on {brand}")

            # Generate events throughout the session
            while session.is_session_active():
                event = session.generate_event()
                event_dict = asdict(event)

                # Send to Kafka
                producer.send(KAFKA_TOPIC, value=event_dict)

                total_events += 1
                events_by_brand[brand] += 1

                # Log every 100 events
                if total_events % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = total_events / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"📊 Stats: {total_events:,} events | " f"{total_sessions:,} sessions | " f"{rate:.1f} events/sec"
                    )

                # Sleep to simulate realistic event rate
                time.sleep(EVENT_RATE_SECONDS)

            # Send session end event
            session_end = session.generate_session_end_event()
            producer.send(KAFKA_TOPIC, value=asdict(session_end))
            total_events += 1
            total_sessions += 1

            logger.info(
                f"✓ Session {session.session_id} ended: "
                f"{len(session.articles_viewed)} articles, "
                f"{session.current_time - session.start_time}s duration"
            )

            user_id += 1

            # Small delay between sessions
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        logger.info("\n\n🛑 Shutting down producer...")
        logger.info(f"📊 Final Stats:")
        logger.info(f"   Total Events: {total_events:,}")
        logger.info(f"   Total Sessions: {total_sessions:,}")
        logger.info(f"   Events by Brand:")
        for brand, count in events_by_brand.items():
            logger.info(f"      {brand}: {count:,}")
        producer.flush()
        producer.close()
        logger.info("✓ Producer closed gracefully")

    except Exception as e:
        logger.error(f"✗ Producer error: {e}")
        import traceback

        traceback.print_exc()
        producer.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
