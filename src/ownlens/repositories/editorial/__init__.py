"""
OwnLens - Editorial Domain Repositories

Repositories for editorial domain entities (authors, articles, content, media, performance, events, etc.).
"""

from .author_repository import AuthorRepository
from .article_repository import ArticleRepository
from .article_content_repository import ArticleContentRepository
from .content_version_repository import ContentVersionRepository
from .media_asset_repository import MediaAssetRepository
from .media_variant_repository import MediaVariantRepository
from .content_media_repository import ContentMediaRepository
from .article_performance_repository import ArticlePerformanceRepository
from .author_performance_repository import AuthorPerformanceRepository
from .category_performance_repository import CategoryPerformanceRepository
from .content_event_repository import ContentEventRepository
from .headline_test_repository import HeadlineTestRepository
from .trending_topic_repository import TrendingTopicRepository
from .content_recommendation_repository import ContentRecommendationRepository
from .media_collection_repository import MediaCollectionRepository
from .media_collection_item_repository import MediaCollectionItemRepository
from .media_usage_repository import MediaUsageRepository

__all__ = [
    "AuthorRepository",
    "ArticleRepository",
    "ArticleContentRepository",
    "ContentVersionRepository",
    "MediaAssetRepository",
    "MediaVariantRepository",
    "ContentMediaRepository",
    "ArticlePerformanceRepository",
    "AuthorPerformanceRepository",
    "CategoryPerformanceRepository",
    "ContentEventRepository",
    "HeadlineTestRepository",
    "TrendingTopicRepository",
    "ContentRecommendationRepository",
    "MediaCollectionRepository",
    "MediaCollectionItemRepository",
    "MediaUsageRepository",
]

