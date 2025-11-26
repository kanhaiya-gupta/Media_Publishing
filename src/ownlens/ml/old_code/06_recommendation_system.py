#!/usr/bin/env python3
"""
Phase 3: Content Recommendation System

This script implements a content recommendation system using:
- Collaborative filtering (Matrix Factorization)
- Content-based filtering
- Hybrid recommendation system

Usage:
    python 06_recommendation_system.py
"""

import json
import pickle
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import NMF
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler, StandardScaler

# OwnLens framework imports
from ownlens.ml.utils import get_clickhouse_client, get_ml_config

# Get script directory for relative paths
SCRIPT_DIR = Path(__file__).parent.absolute()


def connect_to_clickhouse():
    """
    Connect to ClickHouse using ownlens framework configuration.
    
    Returns:
        ClickHouse Client instance or None if connection fails
    """
    client = get_clickhouse_client()
    if client:
        print("✓ Connected to ClickHouse")
    else:
        print("✗ Error connecting to ClickHouse")
    return client


def load_user_article_interactions(client, limit=100000):
    """Load user-article interactions"""
    print("\n" + "=" * 80)
    print("LOADING USER-ARTICLE INTERACTIONS")
    print("=" * 80)

    # First, check what data exists
    print("  Checking available data...")
    try:
        check_query = """
        SELECT 
            count() as total_sessions,
            countIf(article_views > 0 OR article_clicks > 0) as sessions_with_articles,
            countIf(article_ids_visited != '' AND article_ids_visited IS NOT NULL) as sessions_with_article_ids,
            countIf(brand_id != '' AND brand_id IS NOT NULL) as sessions_with_brand
        FROM customer_sessions
        """
        check_results = client.execute(check_query)
        if len(check_results) > 0:
            total, with_articles, with_article_ids, with_brand = check_results[0]
            print(f"    Total sessions: {total:,}")
            print(f"    Sessions with article views/clicks: {with_articles:,}")
            print(f"    Sessions with article_ids_visited: {with_article_ids:,}")
            print(f"    Sessions with brand: {with_brand:,}")

            # Check a sample of article_ids_visited format
            sample_query = """
            SELECT article_ids_visited
            FROM customer_sessions
            WHERE article_ids_visited != '' AND article_ids_visited IS NOT NULL
            LIMIT 1
            """
            sample_results = client.execute(sample_query)
            if len(sample_results) > 0:
                sample = sample_results[0][0]
                print(f"    Sample article_ids_visited format: {sample[:100] if len(sample) > 100 else sample}")
    except Exception as e:
        print(f"    Could not check data: {e}")

    # Try page_analytics first, then fall back to parsing JSON from customer_sessions
    query_page_analytics = f"""
    SELECT 
        user_id,
        article_id,
        count() as interaction_count,
        sum(visit_count) as total_views,
        sum(visit_count) as total_clicks,
        max(created_at) as last_interaction
    FROM page_analytics
    WHERE article_id != '' AND article_id IS NOT NULL
    GROUP BY user_id, article_id
    ORDER BY interaction_count DESC
    LIMIT {limit}
    """

    try:
        results = client.execute(query_page_analytics)
        if len(results) == 0:
            raise Exception("No data in page_analytics")
    except Exception as e:
        # Fall back to customer_sessions: use brand_id/category as article proxies (simpler and reliable)
        print(f"  Note: Using customer_sessions with brand_id/category as article proxies...")
        query_brand = f"""
        SELECT 
            user_id,
            concat(brand_id, '_cat_', toString(unique_categories_count)) as article_id,
            count() as interaction_count,
            sum(article_views) as total_views,
            sum(article_clicks) as total_clicks,
            avg(session_duration_sec) as avg_time_spent,
            max(session_start) as last_interaction
        FROM customer_sessions
        WHERE (article_views > 0 OR article_clicks > 0)
        AND brand_id != '' AND brand_id IS NOT NULL
        GROUP BY user_id, brand_id, unique_categories_count
        ORDER BY interaction_count DESC
        LIMIT {limit}
        """
        try:
            results = client.execute(query_brand)
            if len(results) == 0:
                raise Exception("No brand data")
        except Exception as e2:
            # Final fallback: use session-level aggregations with any brand
            print(f"  Note: Using session-level aggregations (any brand)...")
            query_final = f"""
            SELECT 
                user_id,
                concat('brand_', coalesce(brand_id, 'unknown'), '_cat_', toString(unique_categories_count)) as article_id,
                count() as interaction_count,
                sum(article_views) as total_views,
                sum(article_clicks) as total_clicks,
                avg(session_duration_sec) as avg_time_spent,
                max(session_start) as last_interaction
            FROM customer_sessions
            WHERE article_views > 0 OR article_clicks > 0
            GROUP BY user_id, brand_id, unique_categories_count
            ORDER BY interaction_count DESC
            LIMIT {limit}
            """
            results = client.execute(query_final)

    # Determine column names based on query results
    if len(results) > 0:
        # Check if avg_time_spent is in results (from brand query)
        if len(results[0]) == 7:
            df = pd.DataFrame(
                results,
                columns=[
                    "user_id",
                    "article_id",
                    "interaction_count",
                    "total_views",
                    "total_clicks",
                    "avg_time_spent",
                    "last_interaction",
                ],
            )
        else:
            df = pd.DataFrame(
                results,
                columns=[
                    "user_id",
                    "article_id",
                    "interaction_count",
                    "total_views",
                    "total_clicks",
                    "last_interaction",
                ],
            )
            # Add avg_time_spent as 0 if not present
            df["avg_time_spent"] = 0
    else:
        df = pd.DataFrame(
            columns=[
                "user_id",
                "article_id",
                "interaction_count",
                "total_views",
                "total_clicks",
                "avg_time_spent",
                "last_interaction",
            ]
        )

    print(f"✓ Loaded {len(df):,} user-article interactions")
    print(f"  Unique Users: {df['user_id'].nunique():,}")
    print(f"  Unique Articles: {df['article_id'].nunique():,}")

    # If no data, return None to signal failure
    if len(df) == 0:
        print("\n⚠ Warning: No user-article interactions found!")
        print("  This may be because:")
        print("  - page_analytics table is empty or doesn't exist")
        print("  - article_ids_visited in customer_sessions is empty or in unexpected format")
        print("  - No article interactions have been recorded yet")
        return None

    return df


def create_user_item_matrix(df):
    """Create user-item interaction matrix"""
    print("\n" + "=" * 80)
    print("CREATING USER-ITEM MATRIX")
    print("=" * 80)

    if df is None or len(df) == 0:
        print("⚠ Cannot create matrix: no data available")
        return None

    # Create interaction score (weighted combination)
    df["interaction_score"] = (
        df["total_views"] * 1.0 + df["total_clicks"] * 2.0 + df["avg_time_spent"] / 60.0 * 0.5  # Time in minutes
    )

    # Create user-item matrix
    user_item_matrix = df.pivot_table(index="user_id", columns="article_id", values="interaction_score", fill_value=0)

    print(f"✓ Created user-item matrix")
    print(f"  Shape: {user_item_matrix.shape}")

    # Calculate sparsity safely
    if user_item_matrix.shape[0] > 0 and user_item_matrix.shape[1] > 0:
        sparsity = (user_item_matrix == 0).sum().sum() / (user_item_matrix.shape[0] * user_item_matrix.shape[1]) * 100
        print(f"  Sparsity: {sparsity:.2f}%")
    else:
        print(f"  Sparsity: N/A (empty matrix)")

    return user_item_matrix


def train_collaborative_filtering(user_item_matrix, n_components=50):
    """Train collaborative filtering model using NMF"""
    print("\n" + "=" * 80)
    print("TRAINING COLLABORATIVE FILTERING MODEL")
    print("=" * 80)

    if user_item_matrix is None or user_item_matrix.shape[0] == 0 or user_item_matrix.shape[1] == 0:
        print("⚠ Cannot train model: empty or invalid matrix")
        return None, None, None, None

    # Normalize matrix using MinMaxScaler (NMF requires non-negative values)
    # MinMaxScaler scales to [0, 1] range, which is perfect for NMF
    scaler = MinMaxScaler()
    user_item_scaled = pd.DataFrame(
        scaler.fit_transform(user_item_matrix),
        index=user_item_matrix.index,
        columns=user_item_matrix.columns,
    )

    # Ensure all values are non-negative (should already be, but double-check)
    user_item_scaled = user_item_scaled.clip(lower=0)

    # Non-negative Matrix Factorization
    print(f"\nTraining NMF with {n_components} components...")
    model = NMF(n_components=n_components, random_state=42, max_iter=200)
    W = model.fit_transform(user_item_scaled.fillna(0))
    H = model.components_

    # Reconstruct matrix
    reconstructed = np.dot(W, H)

    print(f"✓ Model trained")
    print(f"  Components: {n_components}")
    print(f"  Reconstruction Error: {model.reconstruction_err_:.2f}")

    return model, W, H, scaler


def get_user_recommendations(user_id, user_item_matrix, model, W, H, n_recommendations=10):
    """Get recommendations for a user"""
    if user_id not in user_item_matrix.index:
        return []

    # Get user index
    user_idx = user_item_matrix.index.get_loc(user_id)

    # Get user's latent factors
    user_factors = W[user_idx]

    # Calculate scores for all items
    item_scores = np.dot(user_factors, H)

    # Get items user hasn't interacted with
    user_interactions = user_item_matrix.loc[user_id]
    unrated_items = user_interactions[user_interactions == 0].index

    # Get scores for unrated items
    unrated_scores = item_scores[user_item_matrix.columns.get_indexer(unrated_items)]

    # Get top recommendations
    top_indices = np.argsort(unrated_scores)[::-1][:n_recommendations]
    recommendations = unrated_items[top_indices]
    scores = unrated_scores[top_indices]

    return list(zip(recommendations, scores))


def create_content_features(client):
    """Create content-based features"""
    print("\n" + "=" * 80)
    print("CREATING CONTENT-BASED FEATURES")
    print("=" * 80)

    # Use same article_id format as load_user_article_interactions (brand + category count)
    query = """
    SELECT 
        concat(brand_id, '_cat_', toString(unique_categories_count)) as article_id,
        brand_id,
        unique_categories_count as category_count,
        count(DISTINCT user_id) as unique_readers,
        sum(article_views) as total_views,
        sum(article_clicks) as total_clicks,
        avg(session_duration_sec) as avg_time_spent,
        count() as total_interactions
    FROM customer_sessions
    WHERE (article_views > 0 OR article_clicks > 0)
    AND brand_id != '' AND brand_id IS NOT NULL
    GROUP BY brand_id, unique_categories_count
    """

    results = client.execute(query)

    df = pd.DataFrame(
        results,
        columns=[
            "article_id",
            "brand",
            "category_count",
            "unique_readers",
            "total_views",
            "total_clicks",
            "avg_time_spent",
            "total_interactions",
        ],
    )

    print(f"✓ Created content features for {len(df):,} articles")

    # One-hot encode brand
    df_encoded = pd.get_dummies(df, columns=["brand"], prefix="brand")

    # Normalize numerical features
    numerical_features = [
        "category_count",
        "unique_readers",
        "total_views",
        "total_clicks",
        "avg_time_spent",
        "total_interactions",
    ]
    scaler = StandardScaler()
    df_encoded[numerical_features] = scaler.fit_transform(df_encoded[numerical_features])

    return df_encoded.set_index("article_id"), scaler


def get_content_based_recommendations(user_id, user_item_matrix, content_features, n_recommendations=10):
    """Get content-based recommendations"""
    print("\n" + "=" * 80)
    print("GETTING CONTENT-BASED RECOMMENDATIONS")
    print("=" * 80)

    # Get user's interacted articles
    user_interactions = user_item_matrix.loc[user_id]
    interacted_articles = user_interactions[user_interactions > 0].index

    if len(interacted_articles) == 0:
        return []

    # Get content features for interacted articles
    interacted_features = content_features.loc[content_features.index.intersection(interacted_articles)]

    if len(interacted_features) == 0:
        return []

    # Calculate average user profile
    user_profile = interacted_features.mean(axis=0)

    # Calculate similarity with all articles
    similarities = cosine_similarity([user_profile], content_features)[0]

    # Get unrated articles
    unrated_articles = content_features.index.difference(interacted_articles)
    unrated_similarities = similarities[content_features.index.get_indexer(unrated_articles)]

    # Get top recommendations
    top_indices = np.argsort(unrated_similarities)[::-1][:n_recommendations]
    recommendations = unrated_articles[top_indices]
    scores = unrated_similarities[top_indices]

    return list(zip(recommendations, scores))


def create_hybrid_recommendations(user_id, collaborative_recs, content_recs, weight_collab=0.6, weight_content=0.4):
    """Create hybrid recommendations"""
    print("\n" + "=" * 80)
    print("CREATING HYBRID RECOMMENDATIONS")
    print("=" * 80)

    # Combine recommendations
    all_recommendations = {}

    # Add collaborative recommendations
    for article_id, score in collaborative_recs:
        all_recommendations[article_id] = score * weight_collab

    # Add content-based recommendations
    for article_id, score in content_recs:
        if article_id in all_recommendations:
            all_recommendations[article_id] += score * weight_content
        else:
            all_recommendations[article_id] = score * weight_content

    # Sort by score
    sorted_recommendations = sorted(all_recommendations.items(), key=lambda x: x[1], reverse=True)

    return sorted_recommendations[:10]


def evaluate_recommendations(user_item_matrix, model, W, H, content_features, n_test_users=100):
    """Evaluate recommendation system"""
    print("\n" + "=" * 80)
    print("EVALUATING RECOMMENDATION SYSTEM")
    print("=" * 80)

    # Sample test users
    test_users = np.random.choice(
        user_item_matrix.index,
        size=min(n_test_users, len(user_item_matrix)),
        replace=False,
    )

    precisions = []
    recalls = []

    for user_id in test_users:
        # Get recommendations
        collab_recs = get_user_recommendations(user_id, user_item_matrix, model, W, H, n_recommendations=10)
        content_recs = get_content_based_recommendations(user_id, user_item_matrix, content_features, n_recommendations=10)
        hybrid_recs = create_hybrid_recommendations(user_id, collab_recs, content_recs)

        # Get actual interactions (for evaluation)
        actual_interactions = set(user_item_matrix.loc[user_id][user_item_matrix.loc[user_id] > 0].index)
        recommended_articles = set([rec[0] for rec in hybrid_recs])

        # Calculate precision and recall (simplified)
        if len(recommended_articles) > 0:
            precision = len(recommended_articles.intersection(actual_interactions)) / len(recommended_articles)
            recalls.append(precision)  # Simplified recall
            precisions.append(precision)

    avg_precision = np.mean(precisions) if precisions else 0
    avg_recall = np.mean(recalls) if recalls else 0

    print(f"\nEvaluation Results:")
    print(f"  Average Precision: {avg_precision:.4f}")
    print(f"  Average Recall: {avg_recall:.4f}")

    return avg_precision, avg_recall


def save_recommendation_model(model, W, H, scaler, content_features, content_scaler):
    """Save recommendation model"""
    print("\n" + "=" * 80)
    print("SAVING RECOMMENDATION MODEL")
    print("=" * 80)

    import os

    models_dir = SCRIPT_DIR / "models"
    os.makedirs(models_dir, exist_ok=True)

    model_file = models_dir / "recommendation_model.pkl"
    with open(model_file, "wb") as f:
        pickle.dump(
            {
                "nmf_model": model,
                "W": W,
                "H": H,
                "scaler": scaler,
                "content_features": content_features,
                "content_scaler": content_scaler,
            },
            f,
        )

    print(f"✓ Saved recommendation model to {model_file}")

    # Save metadata
    metadata = {
        "model_type": "Hybrid Recommendation System",
        "components": ["Collaborative Filtering (NMF)", "Content-Based Filtering"],
        "created_at": datetime.now().isoformat(),
    }

    metadata_file = models_dir / "recommendation_model_metadata.json"
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2, default=str)
    print(f"✓ Saved metadata to {metadata_file}")


def main():
    """Main recommendation system function"""
    print("=" * 80)
    print("MEDIA PUBLISHING - CONTENT RECOMMENDATION SYSTEM")
    print("=" * 80)

    # Connect to ClickHouse
    client = connect_to_clickhouse()
    if not client:
        return

    try:
        # Load user-article interactions
        df = load_user_article_interactions(client, limit=100000)

        if df is None:
            print("\n⚠ Cannot proceed without data. Exiting.")
            return

        # Create user-item matrix
        user_item_matrix = create_user_item_matrix(df)

        if user_item_matrix is None or user_item_matrix.shape[0] == 0 or user_item_matrix.shape[1] == 0:
            print("\n⚠ Cannot proceed without valid user-item matrix. Exiting.")
            return

        # Train collaborative filtering
        model, W, H, scaler = train_collaborative_filtering(user_item_matrix, n_components=50)

        if model is None:
            print("\n⚠ Cannot proceed without trained model. Exiting.")
            return

        # Create content features
        content_features, content_scaler = create_content_features(client)

        # Evaluate system
        precision, recall = evaluate_recommendations(user_item_matrix, model, W, H, content_features)

        # Save model
        save_recommendation_model(model, W, H, scaler, content_features, content_scaler)

        # Example recommendations
        print("\n" + "=" * 80)
        print("EXAMPLE RECOMMENDATIONS")
        print("=" * 80)

        sample_user = user_item_matrix.index[0]
        print(f"\nRecommendations for User {sample_user}:")

        collab_recs = get_user_recommendations(sample_user, user_item_matrix, model, W, H, n_recommendations=10)
        content_recs = get_content_based_recommendations(sample_user, user_item_matrix, content_features, n_recommendations=10)
        hybrid_recs = create_hybrid_recommendations(sample_user, collab_recs, content_recs)

        print(f"\nTop 10 Hybrid Recommendations:")
        for i, (article_id, score) in enumerate(hybrid_recs[:10], 1):
            print(f"  {i:2d}. Article {article_id}: {score:.4f}")

        print("\n" + "=" * 80)
        print("RECOMMENDATION SYSTEM COMPLETE")
        print("=" * 80)
        print(f"\nNext Steps:")
        print("  1. Deploy recommendation API")
        print("  2. Integrate with frontend")
        print("  3. A/B test recommendation strategies")
        print("  4. Monitor recommendation performance")

    except Exception as e:
        print(f"\n✗ Error during recommendation system training: {e}")
        import traceback

        traceback.print_exc()
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
