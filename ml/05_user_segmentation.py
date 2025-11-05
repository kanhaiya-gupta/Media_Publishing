#!/usr/bin/env python3
"""
Phase 2: User Segmentation

This script implements user segmentation using K-Means clustering:
- Feature scaling and PCA
- K-Means clustering
- Segment analysis and visualization
- Segment profiles

Usage:
    python 05_user_segmentation.py
"""

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans, DBSCAN
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, davies_bouldin_score
import matplotlib.pyplot as plt
import seaborn as sns
import json
from datetime import datetime
from pathlib import Path

# Get script directory for relative paths
SCRIPT_DIR = Path(__file__).parent.absolute()

def load_features():
    """Load ML-ready features"""
    print("\n" + "="*80)
    print("LOADING FEATURES")
    print("="*80)
    
    try:
        features_file = SCRIPT_DIR / 'user_features_ml_ready.csv'
        df = pd.read_csv(features_file)
        print(f"✓ Loaded {len(df):,} user features")
        
        feature_list_file = SCRIPT_DIR / 'feature_list.json'
        with open(feature_list_file, 'r') as f:
            feature_list = json.load(f)
        
        print(f"✓ Loaded {len(feature_list)} features")
        return df, feature_list
    except FileNotFoundError:
        print("✗ Features file not found. Run 02_feature_engineering.py first")
        return None, None

def prepare_clustering_features(df, feature_list):
    """Prepare features for clustering"""
    print("\n" + "="*80)
    print("PREPARING CLUSTERING FEATURES")
    print("="*80)
    
    # Select numerical features
    numerical_features = [
        'total_sessions', 'avg_session_duration', 'avg_events_per_session',
        'avg_article_views', 'avg_article_clicks', 'avg_video_plays',
        'avg_pages_per_session', 'avg_categories_diversity',
        'avg_engagement_score', 'session_frequency',
        'engagement_per_session', 'click_through_rate',
        'content_diversity_score', 'brand_loyalty_score',
        'recency_score', 'activity_score', 'days_active'
    ]
    
    # Filter available features
    available_features = [f for f in numerical_features if f in feature_list]
    
    X = df[available_features].copy()
    
    # Handle missing and infinite values
    X = X.fillna(0).replace([np.inf, -np.inf], 0)
    
    print(f"\nSelected Features: {len(available_features)}")
    print(f"  Data Shape: {X.shape}")
    
    return X, available_features

def find_optimal_clusters(X, max_clusters=10):
    """Find optimal number of clusters"""
    print("\n" + "="*80)
    print("FINDING OPTIMAL NUMBER OF CLUSTERS")
    print("="*80)
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Apply PCA for visualization
    pca = PCA(n_components=0.95)  # Keep 95% variance
    X_pca = pca.fit_transform(X_scaled)
    
    print(f"PCA Components: {pca.n_components_} (explains {pca.explained_variance_ratio_.sum()*100:.2f}% variance)")
    
    inertias = []
    silhouette_scores = []
    davies_bouldin_scores = []
    k_range = range(2, max_clusters + 1)
    
    print(f"\nTesting cluster numbers from 2 to {max_clusters}...")
    for k in k_range:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X_pca)
        
        inertias.append(kmeans.inertia_)
        silhouette_scores.append(silhouette_score(X_pca, labels))
        davies_bouldin_scores.append(davies_bouldin_score(X_pca, labels))
        
        print(f"  k={k}: Inertia={kmeans.inertia_:.2f}, Silhouette={silhouette_scores[-1]:.4f}, DB={davies_bouldin_scores[-1]:.4f}")
    
    # Find optimal k (highest silhouette score)
    optimal_k = k_range[np.argmax(silhouette_scores)]
    optimal_silhouette = max(silhouette_scores)
    
    print(f"\n✓ Optimal number of clusters: {optimal_k}")
    print(f"  Silhouette Score: {optimal_silhouette:.4f}")
    
    # Plot elbow curve and silhouette scores
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    
    # Elbow curve
    axes[0].plot(k_range, inertias, 'bo-')
    axes[0].axvline(x=optimal_k, color='r', linestyle='--', label=f'Optimal k={optimal_k}')
    axes[0].set_xlabel('Number of Clusters (k)')
    axes[0].set_ylabel('Inertia')
    axes[0].set_title('Elbow Method')
    axes[0].legend()
    axes[0].grid(True)
    
    # Silhouette scores
    axes[1].plot(k_range, silhouette_scores, 'go-')
    axes[1].axvline(x=optimal_k, color='r', linestyle='--', label=f'Optimal k={optimal_k}')
    axes[1].set_xlabel('Number of Clusters (k)')
    axes[1].set_ylabel('Silhouette Score')
    axes[1].set_title('Silhouette Score')
    axes[1].legend()
    axes[1].grid(True)
    
    # Davies-Bouldin scores
    axes[2].plot(k_range, davies_bouldin_scores, 'mo-')
    axes[2].axvline(x=optimal_k, color='r', linestyle='--', label=f'Optimal k={optimal_k}')
    axes[2].set_xlabel('Number of Clusters (k)')
    axes[2].set_ylabel('Davies-Bouldin Score')
    axes[2].set_title('Davies-Bouldin Score (lower is better)')
    axes[2].legend()
    axes[2].grid(True)
    
    plt.tight_layout()
    k_file = SCRIPT_DIR / 'clustering_optimal_k.png'
    plt.savefig(k_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved optimal k analysis to {k_file}")
    
    return optimal_k, scaler, pca

def perform_clustering(X, n_clusters, scaler, pca):
    """Perform K-Means clustering"""
    print("\n" + "="*80)
    print(f"PERFORMING K-MEANS CLUSTERING (k={n_clusters})")
    print("="*80)
    
    # Scale and transform
    X_scaled = scaler.transform(X)
    X_pca = pca.transform(X_scaled)
    
    # K-Means clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10, max_iter=300)
    labels = kmeans.fit_predict(X_pca)
    
    # Calculate metrics
    silhouette = silhouette_score(X_pca, labels)
    davies_bouldin = davies_bouldin_score(X_pca, labels)
    
    print(f"\nClustering Metrics:")
    print(f"  Silhouette Score: {silhouette:.4f}")
    print(f"  Davies-Bouldin Score: {davies_bouldin:.4f}")
    
    print(f"\nCluster Distribution:")
    unique, counts = np.unique(labels, return_counts=True)
    for cluster, count in zip(unique, counts):
        print(f"  Cluster {cluster}: {count:,} users ({count/len(labels)*100:.2f}%)")
    
    return kmeans, labels

def analyze_segments(df, labels, feature_list):
    """Analyze user segments"""
    print("\n" + "="*80)
    print("SEGMENT ANALYSIS")
    print("="*80)
    
    df['segment'] = labels
    n_clusters = len(np.unique(labels))
    
    # Segment profiles
    segment_profiles = []
    
    for cluster_id in range(n_clusters):
        cluster_data = df[df['segment'] == cluster_id]
        
        profile = {
            'cluster_id': cluster_id,
            'size': len(cluster_data),
            'percentage': len(cluster_data) / len(df) * 100,
            'avg_total_sessions': cluster_data['total_sessions'].mean(),
            'avg_session_duration': cluster_data['avg_session_duration'].mean(),
            'avg_engagement_score': cluster_data.get('avg_engagement_score', cluster_data.get('engagement_per_session', 0)).mean(),
            'avg_article_views': cluster_data['avg_article_views'].mean(),
            'newsletter_signup_rate': cluster_data['has_newsletter'].mean(),
            'avg_content_diversity': cluster_data.get('content_diversity_score', 0).mean(),
            'preferred_brand': cluster_data['preferred_brand'].mode()[0] if 'preferred_brand' in cluster_data.columns else 'N/A',
            'primary_country': cluster_data['primary_country'].mode()[0] if 'primary_country' in cluster_data.columns else 'N/A',
            'primary_device': cluster_data['primary_device'].mode()[0] if 'primary_device' in cluster_data.columns else 'N/A',
            'subscription_tier': cluster_data['current_subscription_tier'].mode()[0] if 'current_subscription_tier' in cluster_data.columns else 'N/A'
        }
        
        segment_profiles.append(profile)
        
        print(f"\nCluster {cluster_id} Profile:")
        print(f"  Size: {profile['size']:,} users ({profile['percentage']:.2f}%)")
        print(f"  Avg Sessions: {profile['avg_total_sessions']:.2f}")
        print(f"  Avg Duration: {profile['avg_session_duration']:.2f} seconds")
        print(f"  Avg Engagement: {profile['avg_engagement_score']:.2f}")
        print(f"  Avg Article Views: {profile['avg_article_views']:.2f}")
        print(f"  Newsletter Signup Rate: {profile['newsletter_signup_rate']:.2%}")
        print(f"  Preferred Brand: {profile['preferred_brand']}")
        print(f"  Primary Country: {profile['primary_country']}")
        print(f"  Primary Device: {profile['primary_device']}")
        print(f"  Subscription Tier: {profile['subscription_tier']}")
    
    return pd.DataFrame(segment_profiles)

def visualize_segments(df, labels, pca, kmeans, scaler, clustering_features):
    """Visualize user segments"""
    print("\n" + "="*80)
    print("VISUALIZING SEGMENTS")
    print("="*80)
    
    # Get PCA components for visualization
    X_pca_2d = pca.transform(scaler.transform(df[[f for f in clustering_features if f in df.columns]].fillna(0)))[:, :2]
    
    # Create visualization
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # PCA scatter plot
    scatter = axes[0, 0].scatter(X_pca_2d[:, 0], X_pca_2d[:, 1], c=labels, cmap='viridis', alpha=0.6)
    axes[0, 0].set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]*100:.2f}% variance)')
    axes[0, 0].set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]*100:.2f}% variance)')
    axes[0, 0].set_title('User Segments (PCA Visualization)')
    axes[0, 0].legend(*scatter.legend_elements(), title="Clusters")
    axes[0, 0].grid(True)
    
    # Cluster sizes
    unique, counts = np.unique(labels, return_counts=True)
    axes[0, 1].bar(unique, counts, color='steelblue')
    axes[0, 1].set_xlabel('Cluster ID')
    axes[0, 1].set_ylabel('Number of Users')
    axes[0, 1].set_title('Cluster Sizes')
    axes[0, 1].grid(True, axis='y')
    
    # Segment engagement comparison
    segment_engagement = df.groupby('segment')['avg_engagement_score'].mean() if 'avg_engagement_score' in df.columns else df.groupby('segment')['engagement_per_session'].mean()
    axes[1, 0].bar(segment_engagement.index, segment_engagement.values, color='orange')
    axes[1, 0].set_xlabel('Cluster ID')
    axes[1, 0].set_ylabel('Avg Engagement Score')
    axes[1, 0].set_title('Average Engagement by Segment')
    axes[1, 0].grid(True, axis='y')
    
    # Segment session frequency
    segment_frequency = df.groupby('segment')['total_sessions'].mean()
    axes[1, 1].bar(segment_frequency.index, segment_frequency.values, color='green')
    axes[1, 1].set_xlabel('Cluster ID')
    axes[1, 1].set_ylabel('Avg Total Sessions')
    axes[1, 1].set_title('Average Sessions by Segment')
    axes[1, 1].grid(True, axis='y')
    
    plt.tight_layout()
    viz_file = SCRIPT_DIR / 'user_segmentation_visualization.png'
    plt.savefig(viz_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved segmentation visualization to {viz_file}")

def save_segments(df, segment_profiles, kmeans, scaler, pca):
    """Save segmentation results"""
    print("\n" + "="*80)
    print("SAVING SEGMENTATION RESULTS")
    print("="*80)
    
    import os
    models_dir = SCRIPT_DIR / 'models'
    os.makedirs(models_dir, exist_ok=True)
    
    # Save user segments
    output_file = SCRIPT_DIR / 'user_segments.csv'
    df[['user_id', 'segment'] + [f for f in df.columns if f not in ['user_id', 'segment']]].to_csv(output_file, index=False)
    print(f"✓ Saved user segments to {output_file}")
    
    # Save segment profiles
    profiles_file = SCRIPT_DIR / 'segment_profiles.csv'
    segment_profiles.to_csv(profiles_file, index=False)
    print(f"✓ Saved segment profiles to {profiles_file}")
    
    # Save model
    import pickle
    model_file = models_dir / 'user_segmentation_model.pkl'
    with open(model_file, 'wb') as f:
        pickle.dump({
            'kmeans': kmeans,
            'scaler': scaler,
            'pca': pca,
            'n_clusters': len(np.unique(df['segment']))
        }, f)
    print(f"✓ Saved segmentation model to {model_file}")
    
    # Save metadata
    metadata = {
        'model_type': 'K-Means Clustering',
        'n_clusters': len(np.unique(df['segment'])),
        'n_users': len(df),
        'created_at': datetime.now().isoformat(),
        'segment_profiles': segment_profiles.to_dict('records')
    }
    
    metadata_file = models_dir / 'user_segmentation_metadata.json'
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    print(f"✓ Saved metadata to {metadata_file}")

def main():
    """Main user segmentation function"""
    print("="*80)
    print("MEDIA PUBLISHING - USER SEGMENTATION")
    print("="*80)
    
    # Load features
    df, feature_list = load_features()
    if df is None:
        return
    
    try:
        # Prepare clustering features
        X, clustering_features = prepare_clustering_features(df, feature_list)
        
        # Find optimal clusters
        optimal_k, scaler, pca = find_optimal_clusters(X, max_clusters=8)
        
        # Perform clustering
        kmeans, labels = perform_clustering(X, optimal_k, scaler, pca)
        
        # Add segment labels to dataframe for visualization
        df_with_segments = df.copy()
        df_with_segments['segment'] = labels
        
        # Analyze segments
        segment_profiles = analyze_segments(df_with_segments.copy(), labels, feature_list)
        
        # Visualize segments
        visualize_segments(df_with_segments.copy(), labels, pca, kmeans, scaler, clustering_features)
        
        # Save results
        save_segments(df_with_segments.copy(), segment_profiles, kmeans, scaler, pca)
        
        print("\n" + "="*80)
        print("USER SEGMENTATION COMPLETE")
        print("="*80)
        print(f"\nSegments Created: {optimal_k}")
        print(f"Total Users: {len(df):,}")
        print(f"\nNext Steps:")
        print("  1. Review segment profiles in segment_profiles.csv")
        print("  2. Use segments for personalized marketing")
        print("  3. Create segment-specific campaigns")
        print("  4. Analyze segment behavior patterns")
        
    except Exception as e:
        print(f"\n✗ Error during segmentation: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

