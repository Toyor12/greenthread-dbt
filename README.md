#### IMPORTANT

Make a copy of the file: commands-scratchpad.txt.example

`cp commands-scratchpad.txt.example commands-scratchpad.txt`

This is where you can put your commands as you make edit to them.


# Deployment Guide - RDS PostgreSQL Integration

This directory contains deployment scripts for adding RDS PostgreSQL with Change Data Capture (CDC) to the Books ETL pipeline.

## Overview

The deployment process consists of two main steps:

1. **deploy-lambdas.sh** - Packages and deploys Lambda function code
2. **deploy-lambdas-rds.sh** - Configures VPC settings and environment variables

## Prerequisites

Before running the deployment scripts, ensure you have:

### 1. AWS Infrastructure
- ✅ VPC with private subnets in 2 availability zones
- ✅ Security group for Lambda (allows outbound to RDS port 5432)
- ✅ RDS PostgreSQL instance running
- ✅ NAT Gateway for internet access (web scraping)
- ✅ S3 VPC Gateway Endpoint (cost optimization)

### 2. AWS Resources
- ✅ S3 bucket: `books-etl-bucket`
- ✅ IAM role: `lambda-etl-execution-role`
- ✅ Secrets Manager secret: `books-etl/rds/credentials`
- ✅ Lambda functions: `extract-books-lambda`, `transform-books-lambda`

### 3. Database Setup
- ✅ PostgreSQL schema deployed (run `schema.sql`)
- ✅ Date dimension populated for 2026-2027
- ✅ Initial exchange rates inserted

### 4. Local Environment
- ✅ AWS CLI configured with credentials
- ✅ Python 3.11 installed
- ✅ pip installed
- ✅ jq installed (optional, for JSON formatting)

## Deployment Steps
Together with teacher's guidance and AI.

**What it does:**
- Installs Python dependencies (requests, beautifulsoup4, psycopg2-binary)
- Creates deployment packages (ZIP files)
- Updates IAM policy with VPC and Secrets Manager permissions
- Deploys Lambda function code
- Runs basic tests

**Expected output:**
```
✓ AWS CLI found
✓ Extract Lambda package created: extract-lambda-v2.zip
✓ Transform Lambda package created: transform-lambda-v2.zip
✓ IAM policy updated
✓ Extract Lambda code updated
✓ Transform Lambda code updated
✓ Extract Lambda test passed
✓ Transform Lambda test passed
```

**Time:** ~5 minutes

---

### Step 2: Configure VPC and RDS Settings

This script configures the Lambda functions with VPC connectivity and RDS credentials.

```bash
./deployment/deploy-lambdas-rds.sh \
  --subnet-a subnet-0abc123 \
  --subnet-b subnet-0def456 \
  --security-group sg-0ghi789 \
  --secret-name books-etl/rds/credentials
```

**Parameters:**
- `--subnet-a`: Private subnet ID in availability zone A
- `--subnet-b`: Private subnet ID in availability zone B
- `--security-group`: Lambda security group ID
- `--secret-name`: Secrets Manager secret name for database credentials

**What it does:**
- Attaches Lambda functions to VPC subnets
- Configures security groups
- Sets environment variables (DB_SECRET_NAME, BUCKET, etc.)
- Updates memory allocations (Extract: 768MB, Transform: 512MB)
- Updates timeouts (Extract: 600s, Transform: 300s)
- Sets reserved concurrency to 1 (prevent parallel runs)
- Runs end-to-end tests

**Expected output:**
```
✓ Extract Lambda configuration complete
✓ Transform Lambda configuration complete
✓ Extract Lambda test passed
✓ Transform Lambda test passed
```

**Time:** ~10 minutes (VPC attachment can be slow)

---

## Verification

After deployment, verify the integration is working:

### 1. Check Lambda Logs

```bash
# Extract Lambda logs
aws logs tail /aws/lambda/extract-books-lambda --follow

# Transform Lambda logs
aws logs tail /aws/lambda/transform-books-lambda --follow
```

Look for:
- ✅ "Database connection test: PASSED"
- ✅ "Inserted X records into staging_books"
- ✅ "CDC processing complete"

### 2. Verify S3 Data

```bash
# Check raw data
aws s3 ls s3://books-etl-bucket/raw/books/ --recursive --human-readable

# Check processed data
aws s3 ls s3://books-etl-bucket/processed/books/ --recursive --human-readable
```

Expected: Files for today's date

### 3. Verify RDS Data

Connect to RDS and run:

```sql
-- Check staging table
SELECT COUNT(*) FROM staging_books WHERE scraped_date = CURRENT_DATE;
-- Expected: 20 records

-- Check current books
SELECT * FROM v_current_books LIMIT 5;

-- Check CDC events
SELECT * FROM v_cdc_summary WHERE detected_at = CURRENT_DATE;

-- Check daily summary
SELECT * FROM fact_daily_summary ORDER BY date_id DESC LIMIT 1;
```

### 4. Check Lambda Metrics

```bash
# View Lambda errors
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Errors \
  --dimensions Name=FunctionName,Value=extract-books-lambda \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 3600 \
  --statistics Sum

# Expected: Sum = 0 (no errors)
```

---

## Manual Testing

To manually trigger the pipeline:

```bash
# 1. Run Extract Lambda
aws lambda invoke \
  --function-name extract-books-lambda \
  --payload '{}' \
  output.json

cat output.json | jq '.'
# Expected: {"status": "success", "records_scraped": 20, ...}

# 2. Wait a few seconds, then run Transform Lambda
aws lambda invoke \
  --function-name transform-books-lambda \
  --payload '{}' \
  output.json

cat output.json | jq '.body | fromjson'
# Expected: {"status": "success", "cdc_summary": {...}, ...}
```

---

## Troubleshooting

### Issue: Lambda timeout

**Symptoms:**
```
Task timed out after 300.00 seconds
```

**Solution:**
- Extract Lambda: Increase timeout to 600s
- Transform Lambda: Increase timeout to 300s

```bash
aws lambda update-function-configuration \
  --function-name extract-books-lambda \
  --timeout 600
```

---

### Issue: VPC connection timeout

**Symptoms:**
```
Unable to connect to RDS
Task timed out
```

**Checklist:**
- ✅ Lambda in private subnets (not public)
- ✅ Security group allows outbound to RDS (port 5432)
- ✅ RDS security group allows inbound from Lambda SG
- ✅ NAT Gateway exists in public subnet (for internet access)
- ✅ Route table configured: 0.0.0.0/0 → NAT Gateway

**Debug:**
```bash
# Check Lambda VPC configuration
aws lambda get-function-configuration \
  --function-name extract-books-lambda \
  --query 'VpcConfig'

# Should show: SubnetIds, SecurityGroupIds, VpcId
```

---

### Issue: Database connection error

**Symptoms:**
```
FATAL: no pg_hba.conf entry for host
FATAL: password authentication failed
```

**Solutions:**

1. **Check secret exists:**
```bash
aws secretsmanager get-secret-value \
  --secret-id books-etl/rds/credentials \
  --query SecretString --output text | jq '.'
```

2. **Verify IAM permissions:**
```bash
aws iam get-role-policy \
  --role-name lambda-etl-execution-role \
  --policy-name lambda-etl-full-access
```

Should include `secretsmanager:GetSecretValue`

3. **Test database connectivity:**
Connect from bastion host or Cloud9:
```bash
psql -h <rds-endpoint> -U etl_admin -d books_etl -c "SELECT 1;"
```

---

### Issue: Module import error

**Symptoms:**
```
No module named 'psycopg2'
No module named 'db_utils'
```

**Solution:**
Re-run deployment script to ensure all dependencies are packaged:

```bash
./deployment/deploy-lambdas.sh
```

---

### Issue: CDC returns no changes

**Symptoms:**
```
cdc_summary: {
  "new_books": 0,
  "price_changes": 0,
  "stock_changes": 0
}
```

**Explanation:**
This is normal if:
- Running on the same day (no changes detected)
- Books already exist in database with same prices

**To test CDC:**
1. Manually modify a price in staging_books:
```sql
UPDATE staging_books
SET price_gbp = 99.99
WHERE title LIKE '%Light%' AND batch_id = (SELECT MAX(batch_id) FROM staging_books);
```

2. Re-run Transform Lambda:
```bash
aws lambda invoke --function-name transform-books-lambda output.json
```

3. Check CDC events:
```sql
SELECT * FROM cdc_events WHERE detected_at = CURRENT_DATE;
```

---

## Rollback Procedure

If deployment fails or causes issues:

### 1. Rollback Lambda Code

```bash
# Revert to previous version
aws lambda update-function-code \
  --function-name extract-books-lambda \
  --zip-file fileb://extract-lambda-v1-backup.zip

aws lambda update-function-code \
  --function-name transform-books-lambda \
  --zip-file fileb://transform-lambda-v1-backup.zip
```

### 2. Remove VPC Configuration

```bash
aws lambda update-function-configuration \
  --function-name extract-books-lambda \
  --vpc-config SubnetIds=[],SecurityGroupIds=[]

aws lambda update-function-configuration \
  --function-name transform-books-lambda \
  --vpc-config SubnetIds=[],SecurityGroupIds=[]
```

### 3. Revert Environment Variables

```bash
aws lambda update-function-configuration \
  --function-name extract-books-lambda \
  --environment 'Variables={BUCKET=books-etl-bucket}'

aws lambda update-function-configuration \
  --function-name transform-books-lambda \
  --environment 'Variables={BUCKET=books-etl-bucket}'
```

**Recovery Time:** 5-10 minutes

---

## Performance Tuning

### Cold Start Optimization

**Issue:** First invocation after deployment takes 5-10 seconds

**Solutions:**
1. Enable Provisioned Concurrency (costs extra):
```bash
aws lambda put-provisioned-concurrency-config \
  --function-name extract-books-lambda \
  --provisioned-concurrent-executions 1
```

2. Use Lambda warming:
Create EventBridge rule to ping Lambda every 5 minutes

### Connection Pool Tuning

**Issue:** "Too many connections" error

**Solution:** Adjust pool size:
```bash
aws lambda update-function-configuration \
  --function-name extract-books-lambda \
  --environment 'Variables={
    BUCKET=books-etl-bucket,
    DB_SECRET_NAME=books-etl/rds/credentials,
    DB_POOL_MIN_SIZE=0,
    DB_POOL_MAX_SIZE=1
  }'
```

---

## Cost Monitoring

Monitor costs with:

```bash
# Lambda costs
aws ce get-cost-and-usage \
  --time-period Start=2026-02-01,End=2026-02-28 \
  --granularity MONTHLY \
  --metrics UnblendedCost \
  --filter file://lambda-cost-filter.json

# RDS costs
aws ce get-cost-and-usage \
  --time-period Start=2026-02-01,End=2026-02-28 \
  --granularity MONTHLY \
  --metrics UnblendedCost \
  --filter '{"Dimensions":{"Key":"SERVICE","Values":["Amazon Relational Database Service"]}}'
```

**Expected monthly costs:**
- RDS: ~$60 (production Multi-AZ)
- Lambda: ~$0.15
- NAT Gateway: ~$32
- Secrets Manager: ~$0.40
- **Total: ~$93/month**

---

## Next Steps

After successful deployment:

1. **Enable CloudWatch Alarms** (see presentation for setup)
2. **Create CloudWatch Dashboard** (see presentation for metrics)
3. **Schedule production runs** (EventBridge already configured)
4. **Monitor for 48 hours** before considering stable
5. **Review presentation** for business use cases and SQL queries

---

## Support

For issues or questions:
1. Check CloudWatch Logs first
2. Verify all prerequisites are met
3. Review troubleshooting section above
4. Check AWS documentation:
   - [Lambda VPC Configuration](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html)
   - [RDS Connectivity](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_VPC.html)
   - [Secrets Manager](https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html)

---

**Version:** 2.0
**Last Updated:** February 2026
**Maintainer:** Data Engineering Team
