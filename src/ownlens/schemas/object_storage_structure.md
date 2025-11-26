# OwnLens - Object Storage Structure

## Overview

This document defines the object storage (S3/MinIO) bucket structure for OwnLens media files.

## Storage Architecture

```
ownlens-media/
├── images/
│   ├── {brand_id}/
│   │   ├── {media_id}/
│   │   │   ├── original.{ext}
│   │   │   ├── thumbnail.{ext}
│   │   │   ├── small.{ext}
│   │   │   ├── medium.{ext}
│   │   │   └── large.{ext}
├── videos/
│   ├── {brand_id}/
│   │   ├── {media_id}/
│   │   │   ├── original.{ext}
│   │   │   ├── thumbnail.jpg
│   │   │   ├── preview.mp4
│   │   │   ├── 720p.mp4
│   │   │   └── 1080p.mp4
├── documents/
│   ├── {brand_id}/
│   │   ├── {media_id}.{ext}
├── audio/
│   ├── {brand_id}/
│   │   ├── {media_id}/
│   │   │   ├── original.{ext}
│   │   │   └── preview.{ext}
└── backups/
    ├── articles/
    │   ├── {article_id}/
    │   │   ├── version_{version_number}.json
    │   │   └── content_backup_{timestamp}.json
    └── media/
        ├── {media_id}/
        │   ├── backup_{timestamp}.{ext}
```

## Path Structure

### Images

**Original**: `images/{brand_id}/{media_id}/original.{ext}`
- Example: `images/bild_123/abc-456-def/original.jpg`

**Thumbnail**: `images/{brand_id}/{media_id}/thumbnail.{ext}`
- Size: 150x150px (square)
- Format: JPEG
- Example: `images/bild_123/abc-456-def/thumbnail.jpg`

**Small**: `images/{brand_id}/{media_id}/small.{ext}`
- Size: 500px width (maintain aspect ratio)
- Format: JPEG or WebP
- Example: `images/bild_123/abc-456-def/small.jpg`

**Medium**: `images/{brand_id}/{media_id}/medium.{ext}`
- Size: 1200px width (maintain aspect ratio)
- Format: JPEG or WebP
- Example: `images/bild_123/abc-456-def/medium.jpg`

**Large**: `images/{brand_id}/{media_id}/large.{ext}`
- Size: 1920px width (maintain aspect ratio)
- Format: JPEG or WebP
- Example: `images/bild_123/abc-456-def/large.jpg`

### Videos

**Original**: `videos/{brand_id}/{media_id}/original.{ext}`
- Example: `videos/bild_123/abc-456-def/original.mp4`

**Thumbnail**: `videos/{brand_id}/{media_id}/thumbnail.jpg`
- Size: 640x360px (16:9 aspect ratio)
- Extracted from video at 10% duration
- Example: `videos/bild_123/abc-456-def/thumbnail.jpg`

**Preview**: `videos/{brand_id}/{media_id}/preview.mp4`
- Duration: First 30 seconds
- Resolution: 480p
- Format: MP4
- Example: `videos/bild_123/abc-456-def/preview.mp4`

**720p**: `videos/{brand_id}/{media_id}/720p.mp4`
- Resolution: 1280x720
- Format: MP4 (H.264)
- Example: `videos/bild_123/abc-456-def/720p.mp4`

**1080p**: `videos/{brand_id}/{media_id}/1080p.mp4`
- Resolution: 1920x1080
- Format: MP4 (H.264)
- Example: `videos/bild_123/abc-456-def/1080p.mp4`

### Documents

**Document**: `documents/{brand_id}/{media_id}.{ext}`
- Example: `documents/bild_123/abc-456-def.pdf`

### Audio

**Original**: `audio/{brand_id}/{media_id}/original.{ext}`
- Example: `audio/bild_123/abc-456-def/original.mp3`

**Preview**: `audio/{brand_id}/{media_id}/preview.{ext}`
- Duration: First 30 seconds
- Format: MP3
- Example: `audio/bild_123/abc-456-def/preview.mp3`

### Backups

**Article Backups**: `backups/articles/{article_id}/version_{version_number}.json`
- Example: `backups/articles/article-123/version_1.json`

**Content Backups**: `backups/articles/{article_id}/content_backup_{timestamp}.json`
- Example: `backups/articles/article-123/content_backup_2024-01-15T10-30-00.json`

**Media Backups**: `backups/media/{media_id}/backup_{timestamp}.{ext}`
- Example: `backups/media/media-123/backup_2024-01-15T10-30-00.jpg`

## URL Generation

### MinIO (Local Development)

```
http://localhost:9000/ownlens-media/images/{brand_id}/{media_id}/original.jpg
```

### S3 (Production)

```
https://s3.amazonaws.com/ownlens-media/images/{brand_id}/{media_id}/original.jpg
```

### CDN (Production with CDN)

```
https://cdn.ownlens.com/images/{brand_id}/{media_id}/original.jpg
```

## File Naming Conventions

### Media IDs

- Format: UUID (e.g., `550e8400-e29b-41d4-a716-446655440000`)
- Generated when media is uploaded
- Used in all paths and URLs

### Brand IDs

- Format: Brand code (e.g., `bild`, `welt`, `business_insider`)
- From `brands.brand_code` table

### File Extensions

- Images: `.jpg`, `.jpeg`, `.png`, `.gif`, `.webp`, `.avif`
- Videos: `.mp4`, `.webm`, `.mov`, `.avi`
- Documents: `.pdf`, `.doc`, `.docx`, `.xls`, `.xlsx`
- Audio: `.mp3`, `.wav`, `.ogg`, `.m4a`

## Storage Policies

### Lifecycle Rules

1. **Original Files**: Keep forever (no expiration)
2. **Variants**: Keep for 1 year if unused
3. **Backups**: Keep for 7 years (compliance)
4. **Deleted Media**: Move to `deleted/` folder, delete after 30 days

### Access Control

1. **Public Media**: Public read access
2. **Private Media**: Authenticated read access
3. **Backups**: Admin-only access

### Versioning

- Enable versioning on bucket
- Keep all versions for compliance
- Auto-delete old versions after 1 year

## Storage Limits

### Per Media Item

- **Images**: Max 50MB per original
- **Videos**: Max 2GB per original
- **Documents**: Max 100MB per file
- **Audio**: Max 200MB per file

### Per Brand

- **Total Storage**: Unlimited (monitored)
- **Monthly Upload**: 1TB per brand (soft limit)

## Backup Strategy

### Article Content Backups

- **Frequency**: On every version change
- **Retention**: 7 years
- **Format**: JSON (full content + metadata)

### Media Backups

- **Frequency**: On upload and major changes
- **Retention**: 7 years
- **Format**: Original file format

## CDN Integration

### CDN Configuration

- **Provider**: CloudFront (AWS) or Cloudflare
- **Cache TTL**: 1 year for variants, 1 hour for originals
- **Compression**: Enable GZIP/Brotli
- **HTTPS**: Required

### CDN URLs

```
https://cdn.ownlens.com/images/{brand_id}/{media_id}/thumbnail.jpg
```

## Migration Strategy

### From Existing Storage

1. **Inventory**: List all existing media files
2. **Mapping**: Map to new structure
3. **Migration**: Copy files to new structure
4. **Update Database**: Update `storage_path` and `storage_url` in database
5. **Verification**: Verify all files accessible
6. **Cleanup**: Delete old storage after verification

## Monitoring

### Metrics to Track

- **Storage Usage**: Per brand, per media type
- **Upload Rate**: Files uploaded per hour/day
- **Access Patterns**: Most accessed media
- **Cost**: Storage and transfer costs
- **Errors**: Failed uploads, access errors

### Alerts

- **Storage Limit**: Alert at 80% capacity
- **Upload Failures**: Alert on upload errors
- **Access Errors**: Alert on 404s or access denied
- **Cost Threshold**: Alert on cost spikes

## Best Practices

1. **Always generate variants** for images (thumbnail, small, medium, large)
2. **Extract thumbnails** for videos automatically
3. **Use CDN** for production media delivery
4. **Enable versioning** for compliance
5. **Monitor storage costs** regularly
6. **Implement lifecycle policies** to manage old files
7. **Use appropriate file formats** (WebP for images, MP4 for videos)
8. **Compress files** before upload when possible
9. **Generate multiple variants** for responsive design
10. **Backup critical media** regularly

