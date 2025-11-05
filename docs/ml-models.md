# ML Models for Media Publishing Analytics

## Overview

This document provides ML model recommendations for various use cases in the Media Publishing real-time analytics pipeline.

## ML Use Cases and Model Recommendations

```mermaid
graph TB
    A[User Analytics Data] --> B[ML Use Cases]
    
    B --> C[Churn Prediction]
    B --> D[Content Recommendation]
    B --> E[Subscription Conversion]
    B --> F[User Segmentation]
    B --> G[Click Prediction]
    B --> H[Engagement Prediction]
    B --> I[Ad Revenue Optimization]
    
    C --> C1[XGBoost/LightGBM]
    D --> D1[Collaborative Filtering]
    D --> D2[Neural Networks]
    E --> E1[Logistic Regression]
    E --> E2[Gradient Boosting]
    F --> F1[K-Means Clustering]
    F --> F2[DBSCAN]
    G --> G1[Wide & Deep]
    G --> G2[FM/FFM]
    H --> H1[Time Series Models]
    I --> I1[Multi-Armed Bandits]
    
    style A fill:#e1f5ff
    style B fill:#e8f5e9
```

## 1. Churn Prediction

### Problem Statement
Predict which users are likely to churn (stop using the platform or unsubscribe) in the next 30 days.

### Recommended Model: XGBoost/LightGBM

```mermaid
flowchart TD
    A[User Features] --> B[Feature Engineering]
    B --> C[Model: XGBoost]
    C --> D[Churn Probability]
    D --> E{Churn Risk?}
    E -->|High| F[Retention Campaign]
    E -->|Low| G[Continue Monitoring]
    
    B --> B1[Session Frequency]
    B --> B2[Engagement Decline]
    B --> B3[Time Since Last Session]
    B --> B4[Subscription Status]
    B --> B5[Content Diversity]
    
    style C fill:#e8f5e9
    style D fill:#f3e5f5
```

### Why XGBoost/LightGBM?
- **Handles non-linear relationships** well
- **Feature importance** for interpretability
- **Handles missing values** automatically
- **Fast training** on large datasets
- **Good performance** on tabular data

### Feature Set
```python
features = [
    'total_sessions',           # Behavioral
    'avg_session_duration',     # Engagement
    'days_since_last_session',  # Recency
    'engagement_score_trend',   # Trend
    'subscription_tier',         # Value
    'newsletter_status',         # Engagement
    'content_diversity',         # Interest
    'device_usage_pattern',     # Behavior
    'geographic_pattern',       # Context
    'session_frequency_decline'  # Churn signal
]
```

### Model Architecture
```mermaid
graph LR
    A[Input Features] --> B[Feature Engineering]
    B --> C[XGBoost Model]
    C --> D[Churn Probability]
    D --> E[Risk Stratification]
    
    E --> F[High Risk: >0.7]
    E --> G[Medium Risk: 0.3-0.7]
    E --> H[Low Risk: <0.3]
    
    style C fill:#e8f5e9
    style D fill:#f3e5f5
```

### Implementation
```python
import xgboost as xgb
from sklearn.model_selection import train_test_split

# Prepare features
X = user_features_df[feature_columns]
y = user_features_df['churned_in_30_days']

# Train model
model = xgb.XGBClassifier(
    objective='binary:logistic',
    n_estimators=1000,
    max_depth=6,
    learning_rate=0.01,
    subsample=0.8,
    colsample_bytree=0.8,
    eval_metric='auc',
    early_stopping_rounds=50
)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
model.fit(X_train, y_train, eval_set=[(X_test, y_test)])
```

## 2. Content Recommendation

### Problem Statement
Recommend articles to users based on their reading history and preferences.

### Recommended Model: Hybrid Recommendation System

```mermaid
graph TB
    A[User Session Data] --> B[Content-Based]
    A --> C[Collaborative Filtering]
    A --> D[Deep Learning]
    
    B --> E[Feature Extraction]
    C --> F[User-Item Matrix]
    D --> G[Neural Network]
    
    E --> H[Hybrid Model]
    F --> H
    G --> H
    
    H --> I[Top-K Recommendations]
    
    style H fill:#e8f5e9
    style I fill:#f3e5f5
```

### Why Hybrid Approach?
- **Content-Based**: Handles cold start (new users/articles)
- **Collaborative Filtering**: Leverages user behavior patterns
- **Deep Learning**: Captures complex interactions

### Model Architecture

#### A. Collaborative Filtering (Matrix Factorization)
```python
from implicit.als import AlternatingLeastSquares

# User-item interaction matrix
model = AlternatingLeastSquares(
    factors=50,
    regularization=0.1,
    iterations=50
)

model.fit(user_item_matrix)
recommendations = model.recommend(user_id, user_item_matrix, N=10)
```

#### B. Deep Learning (Neural Collaborative Filtering)
```python
import tensorflow as tf
from tensorflow import keras

# Wide & Deep model
def build_wide_deep_model():
    # Wide component (linear)
    wide = keras.layers.Dense(1, activation='sigmoid')
    
    # Deep component (non-linear)
    deep = keras.Sequential([
        keras.layers.Dense(128, activation='relu'),
        keras.layers.Dropout(0.3),
        keras.layers.Dense(64, activation='relu'),
        keras.layers.Dropout(0.3),
        keras.layers.Dense(32, activation='relu'),
        keras.layers.Dense(1, activation='sigmoid')
    ])
    
    # Combine
    combined = keras.layers.Concatenate()([wide, deep])
    output = keras.layers.Dense(1, activation='sigmoid')(combined)
    
    return keras.Model(inputs=[wide_input, deep_input], outputs=output)
```

## 3. Subscription Conversion

### Problem Statement
Predict probability of free users converting to paid subscription.

### Recommended Model: Gradient Boosting (LightGBM)

```mermaid
flowchart TD
    A[User Features] --> B[Feature Engineering]
    B --> C[LightGBM Model]
    C --> D[Conversion Probability]
    D --> E{Probability Threshold}
    
    E -->|>0.3| F[Show Premium Prompt]
    E -->|>0.6| G[Aggressive Campaign]
    E -->|<0.3| H[No Action]
    
    B --> B1[Engagement Score]
    B --> B2[Content Consumption]
    B --> B3[Newsletter Signups]
    B --> B4[Ad Click Rate]
    B --> B5[Session Frequency]
    
    style C fill:#e8f5e9
    style D fill:#f3e5f5
```

### Why LightGBM?
- **Fast training** on large datasets
- **Handles categorical features** natively
- **Good for imbalanced data** (conversion rate typically low)
- **Feature importance** for business insights

### Feature Importance
```mermaid
graph LR
    A[Feature Importance] --> B[High]
    A --> C[Medium]
    A --> D[Low]
    
    B --> B1[Engagement Score]
    B --> B2[Newsletter Signups]
    B --> B3[Article Views]
    
    C --> C1[Session Duration]
    C --> C2[Content Diversity]
    C --> C3[Time on Platform]
    
    D --> D1[Device Type]
    D --> D2[Geographic Location]
    
    style B fill:#e8f5e9
```

### Implementation
```python
import lightgbm as lgb

# Prepare data
train_data = lgb.Dataset(
    X_train, 
    label=y_train,
    categorical_feature=['brand', 'country', 'device_type', 'subscription_tier']
)

# Model parameters
params = {
    'objective': 'binary',
    'metric': 'auc',
    'boosting_type': 'gbdt',
    'num_leaves': 31,
    'learning_rate': 0.05,
    'feature_fraction': 0.9,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': 0
}

# Train
model = lgb.train(params, train_data, num_boost_round=1000)
```

## 4. User Segmentation

### Problem Statement
Cluster users into distinct segments for personalized marketing and content delivery.

### Recommended Model: K-Means Clustering with PCA

```mermaid
flowchart TD
    A[User Features] --> B[Feature Scaling]
    B --> C[PCA Dimensionality Reduction]
    C --> D[K-Means Clustering]
    D --> E[User Segments]
    
    E --> F[Power Users]
    E --> G[Casual Users]
    E --> H[New Visitors]
    E --> I[At-Risk Users]
    E --> J[Champions]
    
    style D fill:#e8f5e9
    style E fill:#f3e5f5
```

### Why K-Means with PCA?
- **Interpretable segments** (clear clusters)
- **Handles high-dimensional data** with PCA
- **Fast clustering** on large datasets
- **Easy to visualize** and explain

### Feature Set for Clustering
```python
clustering_features = [
    'avg_session_duration',
    'avg_events_per_session',
    'session_frequency',
    'engagement_score',
    'content_diversity',
    'subscription_tier_encoded',
    'device_usage_pattern',
    'time_pattern_encoded'
]
```

### Implementation
```python
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

# Scale features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(user_features[clustering_features])

# PCA for dimensionality reduction
pca = PCA(n_components=0.95)  # Keep 95% variance
X_pca = pca.fit_transform(X_scaled)

# K-Means clustering
kmeans = KMeans(n_clusters=5, random_state=42, n_init=10)
segments = kmeans.fit_predict(X_pca)

# Analyze segments
user_features['segment'] = segments
segment_analysis = user_features.groupby('segment').agg({
    'avg_session_duration': 'mean',
    'engagement_score': 'mean',
    'subscription_tier': lambda x: x.mode()[0]
})
```

## 5. Click Prediction (Article/Ad)

### Problem Statement
Predict probability of user clicking on an article or ad.

### Recommended Model: Wide & Deep Neural Network

```mermaid
graph TB
    A[User Features] --> B[Wide Component]
    A --> C[Deep Component]
    A --> D[Embedding Layer]
    
    B --> E[Linear Model]
    C --> F[Deep Neural Network]
    D --> G[Categorical Embeddings]
    
    E --> H[Concatenate]
    F --> H
    G --> H
    
    H --> I[Click Probability]
    
    style H fill:#e8f5e9
    style I fill:#f3e5f5
```

### Why Wide & Deep?
- **Wide component**: Memorizes feature interactions
- **Deep component**: Generalizes to unseen combinations
- **Handles sparse features** (categorical) well
- **State-of-the-art** for click prediction

### Model Architecture
```python
import tensorflow as tf
from tensorflow import keras

def build_wide_deep_model():
    # Inputs
    user_features = keras.Input(shape=(num_user_features,), name='user_features')
    article_features = keras.Input(shape=(num_article_features,), name='article_features')
    categorical_features = keras.Input(shape=(num_categorical,), name='categorical')
    
    # Wide component (linear)
    wide = keras.layers.Concatenate()([user_features, article_features])
    wide_output = keras.layers.Dense(1, activation='sigmoid', name='wide_output')(wide)
    
    # Deep component
    deep = keras.layers.Concatenate()([user_features, article_features])
    deep = keras.layers.Dense(128, activation='relu')(deep)
    deep = keras.layers.Dropout(0.3)(deep)
    deep = keras.layers.Dense(64, activation='relu')(deep)
    deep = keras.layers.Dropout(0.3)(deep)
    deep = keras.layers.Dense(32, activation='relu')(deep)
    deep_output = keras.layers.Dense(1, activation='sigmoid', name='deep_output')(deep)
    
    # Embeddings for categorical features
    embeddings = keras.layers.Embedding(vocab_size, embedding_dim)(categorical_features)
    embeddings = keras.layers.Flatten()(embeddings)
    
    # Combine
    combined = keras.layers.Concatenate()([wide_output, deep_output, embeddings])
    output = keras.layers.Dense(1, activation='sigmoid', name='prediction')(combined)
    
    model = keras.Model(
        inputs=[user_features, article_features, categorical_features],
        outputs=output
    )
    
    model.compile(
        optimizer='adam',
        loss='binary_crossentropy',
        metrics=['accuracy', 'auc']
    )
    
    return model
```

## 6. Engagement Prediction

### Problem Statement
Predict user engagement level for the next session.

### Recommended Model: Time Series + Regression

```mermaid
flowchart TD
    A[Historical Sessions] --> B[Time Series Features]
    B --> C[LSTM/GRU]
    C --> D[Engagement Score]
    
    A --> E[Static Features]
    E --> F[Feature Engineering]
    F --> G[Gradient Boosting]
    G --> D
    
    C --> H[Ensemble Model]
    G --> H
    H --> D
    
    style C fill:#e8f5e9
    style H fill:#f3e5f5
```

### Why LSTM/GRU + Gradient Boosting?
- **LSTM/GRU**: Captures temporal patterns in session sequences
- **Gradient Boosting**: Handles static features and non-linear relationships
- **Ensemble**: Best of both worlds

### Implementation
```python
import tensorflow as tf
from tensorflow import keras
import lightgbm as lgb

# LSTM for temporal patterns
def build_lstm_model():
    model = keras.Sequential([
        keras.layers.LSTM(64, return_sequences=True, input_shape=(sequence_length, num_features)),
        keras.layers.Dropout(0.3),
        keras.layers.LSTM(32, return_sequences=False),
        keras.layers.Dropout(0.3),
        keras.layers.Dense(16, activation='relu'),
        keras.layers.Dense(1, activation='linear')  # Regression
    ])
    
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    return model

# Gradient Boosting for static features
lgb_model = lgb.LGBMRegressor(
    n_estimators=1000,
    learning_rate=0.01,
    num_leaves=31,
    objective='regression',
    metric='rmse'
)

# Ensemble
def predict_engagement(sequence_features, static_features):
    lstm_pred = lstm_model.predict(sequence_features)
    lgb_pred = lgb_model.predict(static_features)
    
    # Weighted ensemble
    final_pred = 0.6 * lstm_pred + 0.4 * lgb_pred
    return final_pred
```

## 7. Ad Revenue Optimization

### Problem Statement
Optimize ad placement and bidding to maximize revenue.

### Recommended Model: Multi-Armed Bandit (Thompson Sampling)

```mermaid
flowchart TD
    A[User Context] --> B[Ad Selection]
    B --> C[Thompson Sampling]
    C --> D[Ad Shown]
    D --> E[User Interaction]
    E --> F[Reward Update]
    F --> C
    
    C --> G[Explore]
    C --> H[Exploit]
    
    style C fill:#e8f5e9
    style D fill:#f3e5f5
```

### Why Thompson Sampling?
- **Balances exploration/exploitation** automatically
- **Adapts to changing user preferences** in real-time
- **Maximizes long-term revenue**
- **No offline training** required

### Implementation
```python
import numpy as np
from scipy import stats

class ThompsonSampling:
    def __init__(self, n_ads):
        self.n_ads = n_ads
        self.alpha = np.ones(n_ads)  # Success count
        self.beta = np.ones(n_ads)    # Failure count
    
    def select_ad(self):
        # Sample from Beta distribution for each ad
        samples = [
            np.random.beta(self.alpha[i], self.beta[i])
            for i in range(self.n_ads)
        ]
        return np.argmax(samples)
    
    def update(self, ad, reward):
        if reward > 0:  # Click or conversion
            self.alpha[ad] += 1
        else:
            self.beta[ad] += 1
```

## Model Comparison Matrix

```mermaid
graph TB
    A[Use Case] --> B[Best Model]
    
    subgraph "Churn Prediction"
        C1[XGBoost/LightGBM]
        C2[High Accuracy]
        C3[Feature Importance]
    end
    
    subgraph "Recommendation"
        D1[Hybrid: Matrix Factorization + Deep Learning]
        D2[Handles Cold Start]
        D3[Captures Complex Patterns]
    end
    
    subgraph "Conversion"
        E1[LightGBM]
        E2[Fast Training]
        E3[Handles Imbalance]
    end
    
    subgraph "Segmentation"
        F1[K-Means + PCA]
        F2[Interpretable]
        F3[Fast Clustering]
    end
    
    subgraph "Click Prediction"
        G1[Wide & Deep]
        G2[State-of-the-Art]
        G3[Handles Sparse Features]
    end
    
    subgraph "Engagement"
        H1[LSTM + Gradient Boosting]
        H2[Temporal Patterns]
        H3[Ensemble Power]
    end
    
    subgraph "Ad Optimization"
        I1[Thompson Sampling]
        I2[Real-time Learning]
        I3[Balances Explore/Exploit]
    end
    
    style C1 fill:#e8f5e9
    style D1 fill:#e8f5e9
    style E1 fill:#e8f5e9
    style F1 fill:#e8f5e9
    style G1 fill:#e8f5e9
    style H1 fill:#e8f5e9
    style I1 fill:#e8f5e9
```

## Real-Time ML Pipeline

```mermaid
sequenceDiagram
    participant User as User Session
    participant Kafka as Kafka Stream
    participant Spark as Spark Streaming
    participant FS as Feature Store
    participant ML as ML Models
    participant API as Prediction API
    
    User->>Kafka: Generate Events
    Kafka->>Spark: Stream Events
    Spark->>Spark: Calculate Features
    Spark->>FS: Update Feature Store
    API->>FS: Fetch User Features
    FS->>ML: Return Feature Vector
    ML->>ML: Real-time Prediction
    ML->>API: Return Prediction
    API->>User: Personalized Content
    
    Note over FS: Features updated in real-time
    Note over ML: Predictions made instantly
```

## Model Deployment Strategy

```mermaid
graph TB
    A[Model Training] --> B[Model Validation]
    B --> C{Model Good?}
    C -->|No| A
    C -->|Yes| D[Model Registry]
    D --> E[Model Serving]
    
    E --> F[A/B Testing]
    F --> G{Performance?}
    G -->|Better| H[Promote to Production]
    G -->|Worse| I[Rollback]
    
    H --> J[Monitor Performance]
    J --> K{Drift Detected?}
    K -->|Yes| A
    K -->|No| J
    
    style A fill:#e1f5ff
    style H fill:#e8f5e9
    style J fill:#f3e5f5
```

## Recommended Tech Stack

```mermaid
graph LR
    A[ML Stack] --> B[Training]
    A --> C[Serving]
    A --> D[Monitoring]
    
    B --> B1[XGBoost/LightGBM]
    B --> B2[TensorFlow/PyTorch]
    B --> B3[Scikit-learn]
    
    C --> C1[MLflow]
    C --> C2[TensorFlow Serving]
    C --> C3[Redis Cache]
    
    D --> D1[Prometheus]
    D --> D2[Grafana]
    D --> D3[Evidently AI]
    
    style A fill:#e1f5ff
    style B fill:#e8f5e9
    style C fill:#f3e5f5
    style D fill:#fff4e1
```

## Quick Recommendations Summary

| Use Case | Best Model | Reason |
|----------|-----------|--------|
| **Churn Prediction** | XGBoost/LightGBM | High accuracy, interpretable, handles tabular data well |
| **Content Recommendation** | Hybrid (Matrix Factorization + Deep Learning) | Handles cold start, captures complex patterns |
| **Subscription Conversion** | LightGBM | Fast, handles imbalanced data, categorical features |
| **User Segmentation** | K-Means + PCA | Interpretable, fast, easy to visualize |
| **Click Prediction** | Wide & Deep Neural Network | State-of-the-art, handles sparse features |
| **Engagement Prediction** | LSTM + Gradient Boosting Ensemble | Captures temporal patterns + static features |
| **Ad Optimization** | Thompson Sampling | Real-time learning, balances exploration/exploitation |

## Next Steps

1. **Start with Churn Prediction** (XGBoost) - Highest business impact
2. **Implement Content Recommendation** (Hybrid) - Improves user experience
3. **Add Conversion Prediction** (LightGBM) - Direct revenue impact
4. **Deploy User Segmentation** (K-Means) - Marketing personalization
5. **Optimize with Click Prediction** (Wide & Deep) - Content optimization

