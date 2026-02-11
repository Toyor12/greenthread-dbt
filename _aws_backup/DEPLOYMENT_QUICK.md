# AWS Lambda ETL Pipeline Deployment Guide
# Bereitstellungshandbuch für AWS Lambda ETL-Pipeline

## Architecture Overview | Architekturübersicht

This pipeline implements a two-stage ETL process:
Diese Pipeline implementiert einen zweistufigen ETL-Prozess:

1. **Extract (every 5 minutes)**: Scrape books.toscrape.com → Save to `raw/`
2. **Transform (every 8 minutes)**: Process raw data → Save to `processed/`

---

## Prerequisites | Voraussetzungen

### Required Tools | Erforderliche Tools
- AWS CLI configured with credentials
- Python 3.11+
- AWS account with appropriate permissions

### AWS IAM Permissions | AWS IAM-Berechtigungen
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:CreateFunction",
        "lambda:UpdateFunctionCode",
        "lambda:UpdateFunctionConfiguration",
        "iam:CreateRole",
        "iam:AttachRolePolicy",
        "s3:CreateBucket",
        "s3:PutObject",
        "s3:GetObject",
        "events:PutRule",
        "events:PutTargets"
      ],
      "Resource": "*"
    }
  ]
}
```

---

## Step 1: Create S3 Bucket | Schritt 1: S3-Bucket erstellen

### English
```bash
# Create the bucket
aws s3 mb s3://books-etl-bucket --region eu-central-1

# Verify creation
aws s3 ls | grep books-etl-bucket
```

### Deutsch
```bash
# Bucket erstellen
aws s3 mb s3://books-etl-bucket --region eu-central-1

# Erstellung überprüfen
aws s3 ls | grep books-etl-bucket
```

---

## Step 2: Create IAM Role for Lambda | Schritt 2: IAM-Rolle für Lambda erstellen

### Create Trust Policy | Vertrauensrichtlinie erstellen

Create file `lambda-trust-policy.json`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### Create the Role | Rolle erstellen

```bash
# Create role
aws iam create-role \
  --role-name lambda-etl-execution-role \
  --assume-role-policy-document file://lambda-trust-policy.json

# Attach basic Lambda execution policy
aws iam attach-role-policy \
  --role-name lambda-etl-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

### Create S3 Access Policy | S3-Zugriffsrichtlinie erstellen

Create file `lambda-s3-policy.json`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::books-etl-bucket",
        "arn:aws:s3:::books-etl-bucket/*"
      ]
    }
  ]
}
```

```bash
# Create and attach the policy
aws iam put-role-policy \
  --role-name lambda-etl-execution-role \
  --policy-name lambda-s3-access \
  --policy-document file://lambda-s3-policy.json
```

---

## Step 3: Package Lambda Functions | Schritt 3: Lambda-Funktionen packen

### Extract Function | Extract-Funktion

```bash
# Create package directory
mkdir -p extract-package
cd extract-package

# Install dependencies
pip install requests beautifulsoup4 boto3 -t .

# Copy lambda function
cp ../extract_lambda.py lambda_function.py

# Create deployment package
zip -r ../extract-lambda.zip .
cd ..
```

### Transform Function | Transform-Funktion

```bash
# Create package directory
mkdir -p transform-lambda
cd transform-lambda

# Install dependencies (boto3 is included in Lambda runtime)
# No external dependencies needed for transform

# Copy lambda function
cp ../transform_lambda.py lambda_function.py

# Create deployment package
zip -r ../transform-lambda.zip .
cd ..
```

---

## Step 4: Deploy Lambda Functions | Schritt 4: Lambda-Funktionen bereitstellen

### Extract Lambda

```bash
# Get role ARN
ROLE_ARN=$(aws iam get-role --role-name lambda-etl-execution-role --query 'Role.Arn' --output text)

# Create Extract Lambda function
aws lambda create-function \
  --function-name extract-books-lambda \
  --runtime python3.11 \
  --role $ROLE_ARN \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://extract-lambda.zip \
  --timeout 300 \
  --memory-size 512 \
  --environment Variables="{BUCKET=books-etl-bucket-victor}" \
  --description "Extract books data every 5 minutes"
```

### Transform Lambda

```bash
# Create Transform Lambda function
aws lambda create-function \
  --function-name transform-books-lambda \
  --runtime python3.11 \
  --role $ROLE_ARN \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://transform-lambda.zip \
  --timeout 180 \
  --memory-size 256 \
  --environment Variables="{BUCKET=books-etl-bucket-victor}" \
  --description "Transform books data every 8 minutes"
```

---

## Step 5: Create EventBridge Schedules | Schritt 5: EventBridge-Zeitpläne erstellen

### Extract Schedule (every 5 minutes)

```bash
# Create EventBridge rule
aws events put-rule \
  --name extract-books-morning \
  --schedule-expression "rate(5 minutes)" \
  --state ENABLED \
  --description "Trigger extract Lambda every 5 minutes"
  


# Add Lambda permission
aws lambda add-permission \
  --function-name extract-books-lambda \
  --statement-id extract-books-morning \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:eu-central-1:173063930339:rule/extract-books-morning

# Add Lambda as target
aws events put-targets \
  --rule extract-books-morning \
  --targets "Id"="1","Arn"="arn:aws:lambda:eu-central-1:173063930339:function:extract-books-lambda"
```

### Transform Schedule (every 8 minutes)

```bash
# Create EventBridge rule
aws events put-rule \
  --name transform-books-midnight \
  --schedule-expression "rate(8 minutes)" \
  --state ENABLED \
  --description "Trigger transform Lambda every 8 minutes"

# Add Lambda permission
aws lambda add-permission \
  --function-name transform-books-lambda \
  --statement-id transform-books-midnight \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:eu-central-1:173063930339:rule/transform-books-midnight

# Add Lambda as target
aws events put-targets \
  --rule transform-books-midnight \
  --targets "Id"="1","Arn"="arn:aws:lambda:eu-central-1:173063930339:function:transform-books-lambda"
```

**Note:** Replace `YOUR_ACCOUNT_ID` with your actual AWS account ID.
**Hinweis:** Ersetzen Sie `YOUR_ACCOUNT_ID` durch Ihre tatsächliche AWS-Konto-ID.

---

## Step 6: Test the Pipeline 
## Pipeline testen

### Manual Testing | Manuelles Testen

#### Test Extract Function

```bash
# Invoke extract Lambda manually
aws lambda invoke \
  --function-name extract-books-lambda \
  --payload '{}' \
  extract-output.json

# View response
cat extract-output.json

# Check S3 for raw data
aws s3 ls s3://books-etl-bucket/raw/books/ --recursive
```

#### Test Transform Function

```bash
# Invoke transform Lambda manually
aws lambda invoke \
  --function-name transform-books-lambda \
  --payload '{}' \
  transform-output.json

# View response
cat transform-output.json

# Check S3 for processed data
aws s3 ls s3://books-etl-bucket/processed/books/ --recursive
aws s3 ls s3://books-etl-bucket/processed/summary/ --recursive
```

### View CloudWatch Logs | CloudWatch-Protokolle anzeigen

```bash
# Extract Lambda logs
aws logs tail /aws/lambda/extract-books-lambda --follow

# Transform Lambda logs
aws logs tail /aws/lambda/transform-books-lambda --follow
```

---

## Step 7: Monitor & Maintain | Schritt 7: Überwachen & Warten

### CloudWatch Metrics | CloudWatch-Metriken

Monitor these key metrics:
- Invocations
- Duration
- Errors
- Throttles

```bash
# Get Lambda metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value=extract-books-lambda \
  --start-time 2026-01-29T00:00:00Z \
  --end-time 2026-01-30T00:00:00Z \
  --period 3600 \
  --statistics Sum
```

### Create CloudWatch Alarms | CloudWatch-Alarme erstellen

```bash
# Create alarm for Lambda errors
aws cloudwatch put-metric-alarm \
  --alarm-name extract-lambda-errors \
  --alarm-description "Alert on extract Lambda errors" \
  --metric-name Errors \
  --namespace AWS/Lambda \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --dimensions Name=FunctionName,Value=extract-books-lambda
```

---

## Step 8: Cost Optimization | Schritt 8: Kostenoptimierung

### S3 Lifecycle Policies | S3-Lebenszyklus-Richtlinien

Archive old data to save costs:

Create file `s3-lifecycle-policy.json`:
```json
{
  "Rules": [
    {
      "Id": "ArchiveOldRawData",
      "Status": "Enabled",
      "Prefix": "raw/",
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "GLACIER"
        }
      ],
      "Expiration": {
        "Days": 365
      }
    },
    {
      "Id": "ArchiveOldProcessedData",
      "Status": "Enabled",
      "Prefix": "processed/",
      "Transitions": [
        {
          "Days": 90,
          "StorageClass": "GLACIER"
        }
      ]
    }
  ]
}
```

```bash
# Apply lifecycle policy
aws s3api put-bucket-lifecycle-configuration \
  --bucket books-etl-bucket \
  --lifecycle-configuration file://s3-lifecycle-policy.json
```

---

## Troubleshooting | Fehlerbehebung

### Common Issues | Häufige Probleme

#### 1. Lambda Timeout

**Problem:** Function times out before completion
**Solution:** Increase timeout

```bash
aws lambda update-function-configuration \
  --function-name extract-books-lambda \
  --timeout 600
```

#### 2. Permission Denied | Zugriff verweigert

**Problem:** Lambda cannot write to S3
**Solution:** Verify IAM role permissions

```bash
# Check role policies
aws iam list-attached-role-policies --role-name lambda-etl-execution-role
aws iam list-role-policies --role-name lambda-etl-execution-role
```

#### 3. Module Import Errors | Modulimportfehler

**Problem:** `No module named 'requests'`
**Solution:** Repackage with dependencies

```bash
# Reinstall and repackage
cd extract-package
pip install requests beautifulsoup4 -t . --upgrade
zip -r ../extract-lambda.zip .

# Update function
aws lambda update-function-code \
  --function-name extract-books-lambda \
  --zip-file fileb://extract-lambda.zip
```

---

## Cleanup | Bereinigung

To remove all resources:

```bash
# Delete Lambda functions
aws lambda delete-function --function-name extract-books-lambda
aws lambda delete-function --function-name transform-books-lambda

# Delete EventBridge rules
aws events remove-targets --rule extract-books-morning --ids 1
aws events remove-targets --rule transform-books-midnight --ids 1
aws events delete-rule --name extract-books-morning
aws events delete-rule --name transform-books-midnight

# Delete IAM role
aws iam delete-role-policy --role-name lambda-etl-execution-role --policy-name lambda-s3-access
aws iam detach-role-policy --role-name lambda-etl-execution-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam delete-role --role-name lambda-etl-execution-role

# Empty and delete S3 bucket
aws s3 rm s3://books-etl-bucket --recursive
aws s3 rb s3://books-etl-bucket
```

---

## Advanced Topics | Erweiterte Themen

### 1. Add SNS Notifications | SNS-Benachrichtigungen hinzufügen

```python
import boto3

sns = boto3.client('sns')
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:etl-notifications'

def send_notification(subject, message):
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )

# Add to lambda_handler try/except
except Exception as e:
    send_notification(
        subject='ETL Pipeline Failed',
        message=f'Error: {str(e)}'
    )
```

### 2. Use Secrets Manager for API Keys

```python
import boto3
import json

secrets = boto3.client('secretsmanager')

def get_secret(secret_name):
    response = secrets.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# Usage in Lambda
api_credentials = get_secret('books-scraper-credentials')
```

### 3. Add Data Quality Checks | Datenqualitätsprüfungen hinzufügen

```python
def validate_book_data(books):
    errors = []
    
    for idx, book in enumerate(books):
        # Check required fields
        if not book.get('title'):
            errors.append(f"Row {idx}: Missing title")
        
        # Validate price format
        price = book.get('price', '')
        if not price.startswith('£'):
            errors.append(f"Row {idx}: Invalid price format")
    
    if errors:
        raise ValueError(f"Data quality issues: {errors}")
    
    return True
```
