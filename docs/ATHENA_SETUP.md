# AWS Athena Setup Guide

## ⚠️ Error: Access Denied to Athena

If you're seeing this error:
```
Access denied to Athena. Please check IAM permissions. 
Error: You are not authorized to perform: athena:StartQueryExecution
```

**You need to add Athena permissions to your IAM user.** Follow the steps below.

## Quick Setup

### 1. Create IAM Policy

Create a file `athena-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:BatchCreatePartition",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::garritz-veraset-data-us-west-2",
        "arn:aws:s3:::garritz-veraset-data-us-west-2/*"
      ]
    }
  ]
}
```

### 2. Apply Policy to IAM User

**Quick Setup (Recommended):**
```bash
# Run the setup script
./setup-athena.sh
```

**Manual Setup:**

```bash
# Create policy file
cat > athena-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:BatchCreatePartition",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::garritz-veraset-data-us-west-2",
        "arn:aws:s3:::garritz-veraset-data-us-west-2/*"
      ]
    }
  ]
}
EOF

# Apply to your IAM user (replace 'bliz_krieg' with your actual username)
aws iam put-user-policy \
  --user-name bliz_krieg \
  --policy-name AthenaAnalytics \
  --policy-document file://athena-policy.json
```

**Or attach via AWS Console:**
1. Go to IAM → Users → `bliz_krieg` (or your IAM username)
2. Click "Add permissions" → "Create inline policy"
3. Paste the JSON above
4. Name it "AthenaAnalytics"
5. Save

### 3. Create Glue Database

```bash
aws glue create-database \
  --database-input '{"Name": "veraset"}' \
  --region us-west-2
```

### 4. Verify Setup

```bash
# Check database exists
aws glue get-database --name veraset --region us-west-2

# Check user policies
aws iam list-user-policies --user-name veraset_api
```

## Alternative: Use AWS Console

1. **IAM Permissions**:
   - Go to IAM → Users → Select your user
   - Permissions → Add permissions → Attach policies directly
   - Search for "AmazonAthenaFullAccess" (or create custom policy above)
   - Search for "AWSGlueConsoleFullAccess" (or create custom policy above)

2. **Glue Database**:
   - Go to AWS Glue Console → Databases
   - Click "Add database"
   - Name: `veraset`
   - Click "Create"

## Testing

After setup, try running an analysis again. The first query will:
1. Create the Athena table automatically
2. Load partitions with `MSCK REPAIR TABLE`
3. Run the analysis queries

## Troubleshooting

### Error: "Database veraset not found"
```bash
aws glue create-database --database-input '{"Name": "veraset"}' --region us-west-2
```

### Error: "Access denied"
- Verify IAM policy is attached
- Check policy JSON syntax
- Ensure region matches (us-west-2)

### Error: "Table already exists"
- This is OK - the code handles it
- Partitions will be repaired automatically

### Error: "No query execution ID returned"
- Check Athena service is available in your region
- Verify OUTPUT_LOCATION bucket exists and is accessible

## Cost Notes

- Athena charges ~$5 per TB scanned
- Small datasets (< 100 MB) cost ~$0.0005 per query
- Results are stored in S3 (minimal storage cost)
- First query per dataset: ~10-15s (creates table)
- Subsequent queries: ~2-5s
