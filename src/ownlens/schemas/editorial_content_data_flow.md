# OwnLens - Editorial Content Data Flow

## Overview

This document describes the complete data flow for article creation, publishing, and reading with content and media management.

## Architecture Flow

```
┌─────────────────────────────────────────────────────────┐
│  Author/Editor                                          │
│  - Creates article                                      │
│  - Writes content                                       │
│  - Uploads media                                       │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  Application Layer (API/Service)                        │
│  - Validates content                                    │
│  - Processes media                                      │
│  - Generates variants                                    │
└─────────────────────────────────────────────────────────┘
                          ↓
        ┌─────────────────┴─────────────────┐
        ↓                                   ↓
┌───────────────────────┐      ┌──────────────────────────┐
│  PostgreSQL           │      │  Object Storage (S3/MinIO) │
│  - Article metadata   │      │  - Media files            │
│  - Article content    │      │  - Variants              │
│  - Media metadata     │      │  - Backups                │
│  - Relationships      │      └──────────────────────────┘
│  - Full-text search   │
└───────────────────────┘
```

## Data Flow: Article Creation

### Step 1: Create Article Metadata

```sql
-- Insert article metadata
INSERT INTO editorial_articles (
    article_id,
    brand_id,
    title,
    headline,
    summary,
    content_url,
    article_type,
    primary_author_id,
    primary_category_id,
    publish_time,
    status
) VALUES (...);
```

**Result**: Article record created in `editorial_articles` table

---

### Step 2: Upload Media Files

**2.1 Upload to Object Storage**

```
POST /api/media/upload
- File: image.jpg
- Brand: bild
- Media Type: image
```

**Storage Path**: `images/bild/{media_id}/original.jpg`

**2.2 Create Media Metadata**

```sql
-- Insert media metadata
INSERT INTO editorial_media_assets (
    media_id,
    brand_id,
    media_name,
    media_type,
    original_filename,
    file_extension,
    mime_type,
    file_size_bytes,
    storage_path,
    storage_url,
    image_width,
    image_height,
    uploaded_by
) VALUES (...);
```

**2.3 Generate Variants**

- **Thumbnail**: 150x150px → `images/bild/{media_id}/thumbnail.jpg`
- **Small**: 500px width → `images/bild/{media_id}/small.jpg`
- **Medium**: 1200px width → `images/bild/{media_id}/medium.jpg`
- **Large**: 1920px width → `images/bild/{media_id}/large.jpg`

**2.4 Store Variant Metadata**

```sql
-- Insert media variants
INSERT INTO editorial_media_variants (
    media_id,
    variant_type,
    storage_path,
    storage_url,
    width,
    height,
    file_size_bytes
) VALUES (...);
```

**Result**: Media files stored in object storage, metadata in PostgreSQL

---

### Step 3: Write Article Content

**3.1 Create Content Version**

```sql
-- Insert article content
INSERT INTO editorial_article_content (
    article_id,
    brand_id,
    content_format,
    content_body,
    content_body_json,
    content_blocks,
    content_status,
    is_current_version,
    version_created_by
) VALUES (...);
```

**Automatic Triggers**:
- ✅ Search vector generated (tsvector)
- ✅ Keywords extracted
- ✅ Word count calculated
- ✅ Reading time estimated
- ✅ Version number assigned

**3.2 Create Version History**

```sql
-- Insert version record
INSERT INTO editorial_content_versions (
    article_id,
    content_id,
    brand_id,
    version_number,
    version_type,
    version_status,
    created_by
) VALUES (...);
```

**Result**: Content stored in PostgreSQL with full-text search index

---

### Step 4: Link Media to Article

**4.1 Create Content-Media Relationship**

```sql
-- Link media to article
INSERT INTO editorial_content_media (
    article_id,
    media_id,
    content_id,
    relationship_type,
    position_in_content,
    media_caption,
    media_alt_text,
    is_featured
) VALUES (...);
```

**Automatic Triggers**:
- ✅ Media usage count incremented
- ✅ Last used timestamp updated

**Result**: Media linked to article with position and context

---

## Data Flow: Article Publishing

### Step 1: Review and Approve

```sql
-- Update content status
UPDATE editorial_article_content
SET content_status = 'approved',
    is_published_version = TRUE
WHERE content_id = ?;
```

---

### Step 2: Publish Article

**2.1 Update Article Status**

```sql
-- Publish article
UPDATE editorial_articles
SET status = 'published',
    publish_time = CURRENT_TIMESTAMP
WHERE article_id = ?;
```

**2.2 Mark Content as Published**

```sql
-- Mark content as published
UPDATE editorial_article_content
SET is_published_version = TRUE,
    published_at = CURRENT_TIMESTAMP
WHERE content_id = ?;
```

**2.3 Update Version Status**

```sql
-- Update version status
UPDATE editorial_content_versions
SET version_status = 'published',
    published_at = CURRENT_TIMESTAMP,
    published_by = ?
WHERE content_id = ?;
```

**Result**: Article published and searchable

---

## Data Flow: Article Reading

### Step 1: Query Article Metadata

```sql
-- Get article metadata
SELECT 
    article_id,
    title,
    headline,
    summary,
    publish_time,
    primary_author_id,
    primary_category_id
FROM editorial_articles
WHERE article_id = ?;
```

---

### Step 2: Query Article Content

```sql
-- Get current published content
SELECT 
    content_body,
    content_body_json,
    content_blocks,
    word_count,
    reading_time_minutes
FROM editorial_article_content
WHERE article_id = ?
  AND is_current_version = TRUE
  AND is_published_version = TRUE;
```

---

### Step 3: Query Article Media

```sql
-- Get article media using helper function
SELECT * FROM get_article_media(article_id, 'image');
```

**Or direct query**:

```sql
-- Get article media
SELECT 
    m.media_id,
    m.media_name,
    m.media_type,
    cm.relationship_type,
    cm.position_in_content,
    m.storage_url,
    get_media_url(m.media_id, 'thumbnail') AS thumbnail_url,
    cm.media_caption,
    COALESCE(cm.media_alt_text, m.alt_text) AS alt_text
FROM editorial_content_media cm
INNER JOIN editorial_media_assets m ON cm.media_id = m.media_id
WHERE cm.article_id = ?
  AND m.is_active = TRUE
ORDER BY cm.position_in_content ASC;
```

---

### Step 4: Generate Media URLs

**For each media item**:

1. **Get variant URL** (e.g., thumbnail):
   ```sql
   SELECT get_media_url(media_id, 'thumbnail');
   ```

2. **Or construct URL**:
   ```
   https://cdn.ownlens.com/images/{brand_id}/{media_id}/thumbnail.jpg
   ```

---

## Data Flow: Full-Text Search

### Step 1: Search Articles

```sql
-- Search articles by content
SELECT * FROM search_articles_by_content(
    'artificial intelligence',
    brand_id_filter := 'bild',
    limit_results := 20
);
```

**Or direct query**:

```sql
-- Full-text search
SELECT 
    a.article_id,
    a.title,
    a.headline,
    c.content_body,
    ts_rank(c.search_vector, plainto_tsquery('english', 'search query')) AS relevance
FROM editorial_articles a
INNER JOIN editorial_article_content c ON a.article_id = c.article_id
WHERE 
    c.is_current_version = TRUE
    AND c.is_published_version = TRUE
    AND c.search_vector @@ plainto_tsquery('english', 'search query')
ORDER BY relevance DESC;
```

---

## Data Flow: Media Management

### Upload Media

1. **Upload file** to object storage
2. **Extract metadata** (dimensions, duration, etc.)
3. **Generate variants** (thumbnails, different sizes)
4. **Store metadata** in PostgreSQL
5. **Store variants** in object storage
6. **Link variants** in PostgreSQL

### Delete Media

1. **Check usage** (is media used in articles?)
2. **If used**: Soft delete (mark as inactive)
3. **If unused**: Hard delete
4. **Delete from object storage** (original + variants)
5. **Delete from PostgreSQL** (metadata + relationships)

### Update Media

1. **Upload new file** to object storage
2. **Generate new variants**
3. **Update metadata** in PostgreSQL
4. **Keep old versions** for rollback (if needed)

---

## Performance Optimizations

### 1. Content Caching

- Cache article content in Redis/Memcached
- Cache key: `article:{article_id}:content`
- TTL: 1 hour (or until updated)

### 2. Media URL Caching

- Cache media URLs in Redis
- Cache key: `media:{media_id}:url:{variant}`
- TTL: 24 hours

### 3. Search Indexing

- Use PostgreSQL tsvector (already indexed)
- Consider Elasticsearch for advanced search (optional)
- Update search index on content changes

### 4. CDN for Media

- Use CDN for all media delivery
- Cache variants aggressively (1 year TTL)
- Cache originals moderately (1 hour TTL)

---

## Error Handling

### Upload Failures

1. **Retry logic**: 3 retries with exponential backoff
2. **Fallback**: Store in temporary location
3. **Notification**: Alert on persistent failures

### Storage Failures

1. **Replication**: Store in multiple regions
2. **Backup**: Regular backups to secondary storage
3. **Monitoring**: Alert on storage errors

### Database Failures

1. **Transactions**: Use transactions for consistency
2. **Rollback**: Rollback on errors
3. **Logging**: Log all errors for debugging

---

## Monitoring

### Metrics to Track

- **Article Creation Rate**: Articles created per hour/day
- **Media Upload Rate**: Media uploaded per hour/day
- **Content Size**: Average article size
- **Media Size**: Average media file size
- **Search Performance**: Search query latency
- **Storage Usage**: Storage used per brand
- **CDN Hit Rate**: CDN cache hit percentage

### Alerts

- **Upload Failures**: Alert on upload errors
- **Storage Limit**: Alert at 80% capacity
- **Search Slowdown**: Alert on slow searches
- **CDN Errors**: Alert on CDN failures

---

## Best Practices

1. **Always use transactions** for multi-table operations
2. **Generate variants** immediately after upload
3. **Use CDN** for all media delivery
4. **Cache content** for frequently accessed articles
5. **Monitor storage costs** regularly
6. **Backup content** on every version change
7. **Use appropriate file formats** (WebP for images)
8. **Compress files** before upload
9. **Index search vectors** automatically
10. **Track media usage** for analytics

