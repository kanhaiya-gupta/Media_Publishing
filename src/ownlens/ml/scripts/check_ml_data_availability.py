#!/usr/bin/env python3
"""
OwnLens - ML Module: Check ML Data Availability

Script to check if there's enough data in ClickHouse to run ML models.

Usage:
    python -m ownlens.ml.scripts.check_ml_data_availability
"""

import sys
from pathlib import Path
from datetime import date, timedelta
import logging

# Load environment variables from .env file
from dotenv import load_dotenv
project_root = Path(__file__).parent.parent.parent.parent.parent
env_file = project_root / "development.env"
if env_file.exists():
    load_dotenv(env_file)
    print(f"✓ Loaded environment variables from {env_file}")
else:
    print(f"⚠ Warning: development.env not found at {env_file}. Using system environment variables.")

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from ownlens.ml.utils.logging import setup_logging
from ownlens.ml.utils.config import get_ml_config
from clickhouse_driver import Client

# Setup logging
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_data_availability():
    """Check if there's enough data to run ML models."""
    config = get_ml_config()
    
    # Try to connect with native port (9002 or 9000)
    # clickhouse-driver needs native protocol port, not HTTP port (8123)
    native_ports = [9002, 9000]
    client = None
    
    for native_port in native_ports:
        try:
            conn_params = config.get_clickhouse_connection_params(use_native_port=True)
            conn_params['port'] = native_port
            client = Client(**conn_params)
            client.execute("SELECT 1")  # Test connection
            logger.info(f"✅ Connected to ClickHouse on port {native_port}")
            break
        except Exception as e:
            if client:
                try:
                    client.disconnect()
                except:
                    pass
            client = None
            if native_port == native_ports[-1]:  # Last port attempt
                logger.error(f"❌ Failed to connect to ClickHouse on ports {native_ports}: {e}")
                raise Exception(f"Could not connect to ClickHouse on ports {native_ports}: {e}")
            continue
    
    if client is None:
        raise Exception("Failed to establish ClickHouse connection")
    
    logger.info("=" * 80)
    logger.info("CHECKING ML DATA AVAILABILITY")
    logger.info("=" * 80)
    
    try:
        # Check customer domain data
        logger.info("\n📊 Customer Domain Data:")
        logger.info("-" * 80)
        
        # Check user features
        query = "SELECT COUNT(*) FROM customer_user_features"
        result = client.execute(query)
        user_features_count = result[0][0] if result else 0
        logger.info(f"  ✅ customer_user_features: {user_features_count:,} rows")
        
        # Check sessions
        query = "SELECT COUNT(*) FROM customer_sessions"
        result = client.execute(query)
        sessions_count = result[0][0] if result else 0
        logger.info(f"  ✅ customer_sessions: {sessions_count:,} rows")
        
        # Check events
        query = "SELECT COUNT(*) FROM customer_events"
        result = client.execute(query)
        events_count = result[0][0] if result else 0
        logger.info(f"  ✅ customer_events: {events_count:,} rows")
        
        # Check editorial domain data
        logger.info("\n📊 Editorial Domain Data:")
        logger.info("-" * 80)
        
        # Check articles
        query = "SELECT COUNT(*) FROM editorial_articles"
        result = client.execute(query)
        articles_count = result[0][0] if result else 0
        logger.info(f"  ✅ editorial_articles: {articles_count:,} rows")
        
        # Check article performance
        query = "SELECT COUNT(*) FROM editorial_article_performance"
        result = client.execute(query)
        article_perf_count = result[0][0] if result else 0
        logger.info(f"  ✅ editorial_article_performance: {article_perf_count:,} rows")
        
        # Check content events
        query = "SELECT COUNT(*) FROM editorial_content_events"
        result = client.execute(query)
        content_events_count = result[0][0] if result else 0
        logger.info(f"  ✅ editorial_content_events: {content_events_count:,} rows")
        
        # Check author performance
        query = "SELECT COUNT(*) FROM editorial_author_performance"
        result = client.execute(query)
        author_perf_count = result[0][0] if result else 0
        logger.info(f"  ✅ editorial_author_performance: {author_perf_count:,} rows")
        
        # Check category performance
        query = "SELECT COUNT(*) FROM editorial_category_performance"
        result = client.execute(query)
        category_perf_count = result[0][0] if result else 0
        logger.info(f"  ✅ editorial_category_performance: {category_perf_count:,} rows")
        
        # Determine if we can run ML
        logger.info("\n" + "=" * 80)
        logger.info("ML READINESS ASSESSMENT")
        logger.info("=" * 80)
        
        customer_ready = user_features_count >= 100 and sessions_count >= 100
        editorial_ready = articles_count >= 50 and article_perf_count >= 50
        
        if customer_ready:
            logger.info("✅ Customer Domain: READY FOR ML")
            logger.info(f"   - User features: {user_features_count:,} (minimum: 100)")
            logger.info(f"   - Sessions: {sessions_count:,} (minimum: 100)")
        else:
            logger.warning("⚠️  Customer Domain: NEEDS MORE DATA")
            logger.warning(f"   - User features: {user_features_count:,} (minimum: 100)")
            logger.warning(f"   - Sessions: {sessions_count:,} (minimum: 100)")
        
        if editorial_ready:
            logger.info("✅ Editorial Domain: READY FOR ML")
            logger.info(f"   - Articles: {articles_count:,} (minimum: 50)")
            logger.info(f"   - Article performance: {article_perf_count:,} (minimum: 50)")
        else:
            logger.warning("⚠️  Editorial Domain: NEEDS MORE DATA")
            logger.warning(f"   - Articles: {articles_count:,} (minimum: 50)")
            logger.warning(f"   - Article performance: {article_perf_count:,} (minimum: 50)")
        
        # Recommendations
        logger.info("\n" + "=" * 80)
        logger.info("RECOMMENDATIONS")
        logger.info("=" * 80)
        
        if customer_ready:
            logger.info("✅ You can run customer domain ML models:")
            logger.info("   python -m ownlens.ml.scripts.run_complete_ml_workflow --domain customer --limit 1000")
        
        if editorial_ready:
            logger.info("✅ You can run editorial domain ML models:")
            logger.info("   python -m ownlens.ml.scripts.run_complete_ml_workflow --domain editorial --limit 500")
        
        if customer_ready and editorial_ready:
            logger.info("\n✅ You can run ALL ML models:")
            logger.info("   python -m ownlens.ml.scripts.run_complete_ml_workflow --domain customer")
            logger.info("   python -m ownlens.ml.scripts.run_complete_ml_workflow --domain editorial")
        
        if not customer_ready and not editorial_ready:
            logger.warning("\n⚠️  You need more data before running ML models.")
            logger.warning("   Please run ETL pipeline to load more data first.")
        
        logger.info("=" * 80)
        
        return {
            'customer_ready': customer_ready,
            'editorial_ready': editorial_ready,
            'user_features_count': user_features_count,
            'sessions_count': sessions_count,
            'articles_count': articles_count,
            'article_perf_count': article_perf_count
        }
        
    finally:
        client.disconnect()


def main():
    """Main function."""
    result = check_data_availability()
    return result


if __name__ == "__main__":
    main()

