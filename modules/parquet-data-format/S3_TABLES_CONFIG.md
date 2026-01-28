# S3 Tables Configuration Guide

## Quick Setup

Your OpenSearch installation now uses **AWS S3 Tables** instead of AWS Glue Catalog for Iceberg metadata.

### Required System Properties

Add these to your OpenSearch startup configuration:

```bash
# S3 Tables bucket ARN (REQUIRED)
-Diceberg.s3tables.bucket.arn=arn:aws:s3tables:us-west-2:YOUR_ACCOUNT_ID:bucket/YOUR_BUCKET_NAME

# Warehouse location in S3 (REQUIRED)
-Diceberg.warehouse=s3://YOUR_BUCKET_NAME/warehouse

# AWS Region (REQUIRED)
-Daws.region=us-west-2
```

### Example Configuration

```bash
export OPENSEARCH_JAVA_OPTS="-Diceberg.s3tables.bucket.arn=arn:aws:s3tables:us-west-2:123456789012:bucket/opensearch-iceberg-tables \
  -Diceberg.warehouse=s3://opensearch-iceberg-tables/warehouse \
  -Daws.region=us-west-2"
```

### AWS Permissions Required

Your AWS credentials need:
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on the S3 Tables bucket
- `s3tables:*` permissions for table operations

### Key Differences from Glue Catalog

| Feature | Glue Catalog (Old) | S3 Tables (New) |
|---------|-------------------|-----------------|
| Metadata Storage | AWS Glue | S3 bucket |
| API Endpoint | `glue.{region}.amazonaws.com` | `s3tables.{region}.amazonaws.com` |
| Permissions | `glue:*` | S3 permissions only |
| Cost | Per API call | S3 storage only |

### Verification

Check logs for successful initialization:
```
[Iceberg S3Tables] Initialized S3 Tables catalog: endpoint=https://s3tables.us-west-2.amazonaws.com
```

### Troubleshooting

**Issue**: `Table not found in S3 Tables`
- **Solution**: Ensure S3 Tables bucket ARN is correct and accessible

**Issue**: `Failed to initialize S3 Tables catalog`
- **Solution**: Check AWS credentials and region configuration

**Issue**: Authentication errors
