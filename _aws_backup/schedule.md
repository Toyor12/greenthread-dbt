# New Schedule

After 5 minutes -- extract book price in GBP (pounds)

After 10 minutes -- transform - (Getting the EUR rate and storing it in the specified format)

# Every 5 minutes
aws events put-rule \
  --name extract-books-morning-every-5-minutes \
  --schedule-expression "rate(5 minutes)" \
  --state ENABLED \
  --description "Trigger extract Lambda every 5 minutes"

# Add Lambda permission
aws lambda add-permission \
  --function-name extract-books-lambda \
  --statement-id extract-books-every-5-minutes \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn <EVENT BRIDGE ARN>

# Add Lambda as target
aws events put-targets \
  --rule extract-books-every-5-minutes \
  --targets "Id"="1","Arn"="<FUNCTION LAMBDA ARN>"


# Transform
## Create EventBridge rule
aws events put-rule \
  --name transform-books-8-minutes \
  --schedule-expression "rate(8 minutes)" \
  --state ENABLED \
  --description "Trigger transform Lambda every 8 minutes"

# Add Lambda permission
aws lambda add-permission \
  --function-name transform-books-lambda \
  --statement-id transform-books-8-minutes \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn <EVENT BRIDGE ARN>

# Add Lambda as target
aws events put-targets \
  --rule transform-books-midnight-10-minutes \
  --targets "Id"="1","Arn"="<FUNCTION LAMBDA ARN>"  