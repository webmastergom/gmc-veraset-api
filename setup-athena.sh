#!/bin/bash
# Setup script for AWS Athena permissions
# User: bliz_krieg

echo "🔧 Setting up AWS Athena permissions for user: bliz_krieg"

# Create policy file
cat > /tmp/athena-policy.json << 'EOF'
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

echo "📝 Policy file created at /tmp/athena-policy.json"

# Apply policy to IAM user
echo "🔐 Applying policy to IAM user: bliz_krieg"
aws iam put-user-policy \
  --user-name bliz_krieg \
  --policy-name AthenaAnalytics \
  --policy-document file:///tmp/athena-policy.json

if [ $? -eq 0 ]; then
  echo "✅ Policy applied successfully"
else
  echo "❌ Failed to apply policy. Check AWS credentials and permissions."
  exit 1
fi

# Create Glue database
echo "🗄️  Creating Glue database: veraset"
aws glue create-database \
  --database-input '{"Name": "veraset"}' \
  --region us-west-2

if [ $? -eq 0 ]; then
  echo "✅ Database created successfully"
elif [ $? -eq 254 ]; then
  echo "ℹ️  Database already exists (this is OK)"
else
  echo "❌ Failed to create database. Check AWS credentials and permissions."
  exit 1
fi

# Verify setup
echo ""
echo "🔍 Verifying setup..."
echo ""

echo "Checking IAM policies:"
aws iam list-user-policies --user-name bliz_krieg

echo ""
echo "Checking Glue database:"
aws glue get-database --name veraset --region us-west-2

echo ""
echo "✨ Setup complete! You can now run dataset analysis."
echo ""
echo "Next steps:"
echo "1. Restart your Next.js dev server (if running)"
echo "2. Go to /datasets in your app"
echo "3. Click 'Run Analysis' on any dataset"
