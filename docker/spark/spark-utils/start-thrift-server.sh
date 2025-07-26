#!/bin/bash

# 1. Stop any running Thrift Server
# ---------------------------------------------------
echo "Checking if Spark Thrift Server is already running..."

RUNNING_SERVER_PIDS=$(pgrep -f HiveThriftServer2)
if [[ -n "$RUNNING_SERVER_PIDS" ]]; then
  echo "Found running Spark Thrift Server (PIDs: $RUNNING_SERVER_PIDS). Stopping..."
  kill "$RUNNING_SERVER_PIDS"
  sleep 3
  # Optionally force kill if still alive
  RUNNING_SERVER_PIDS=$(pgrep -f HiveThriftServer2)
  if [[ -n "$RUNNING_SERVER_PIDS" ]]; then
    echo "Forcing kill of remaining Thrift Server processes..."
    kill -9 "$RUNNING_SERVER_PIDS"
  fi
else
  echo "No running Spark Thrift Server found."
fi

# 2. Fetch secrets from AWS Secrets Manager
# ---------------------------------------------------
# SECRET_JSON=$(aws secretsmanager get-secret-value \
#   --secret-id dss-da-dev-sgd-secret-glue-conn-sf \
#   --query SecretString \
#   --output text \
#   --region us-west-2)

# # Extract individual values
# SNOWFLAKE_USER=$(echo "$SECRET_JSON" | jq -r '.USER')
# SNOWFLAKE_PASSWORD=$(echo "$SECRET_JSON" | jq -r '.PASSWORD')
# SNOWFLAKE_URI=$(echo "$SECRET_JSON" | jq -r '.sfUri')
# SNOWFLAKE_ROLE=$(echo "$SECRET_JSON" | jq -r '.sfRole')

# 3. Set log file path
# ---------------------------------------------------
THRIFT_SERVER_LOG="/tmp/spark-thrift-server.log"

# 4. Start Spark Thrift Server in the background using nohup + disown
# ---------------------------------------------------
echo "Starting Spark Thrift Server..."

# Remove or truncate existing log
: > "$THRIFT_SERVER_LOG"

# Run spark-submit via nohup so it remains alive after this script exits
nohup spark-submit --deploy-mode client --master local[*] --driver-memory 8g --executor-memory 8g \
  --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
  /usr/lib/spark/jars/spark-sql_2.12.jar \
 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.glue:glue-extensions-for-iceberg-spark-runtime-3.5_2.12:0.3.0 \
 --conf spark.hadoop.fs.s3a.aws.region="$AWS_REGION"  \
 --conf spark.hadoop.fs.s3a.endpoint="s3.$AWS_REGION.amazonaws.com"  \
 --conf spark.hadoop.fs.s3a.access.key="$AWS_ACCESS_KEY" \
 --conf spark.hadoop.fs.s3a.secret.key="$AWS_SECRET_ACCESS_KEY" \
 --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem  \
 --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory \
 --conf spark.hive.imetastoreclient.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory  \
 --conf spark.sql.catalogImplementation=hive \
 --conf spark.sql.defaultCatalog=spark_catalog \
 --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
 --conf spark.sql.catalog.spark_catalog.warehouse="$DBT_GLUE_ICEBERG_WAREHOUSE" \
 --conf spark.sql.catalog.spark_catalog.type=glue  \
 --conf spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO  \
 --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog  \
 --conf spark.sql.catalog.glue_catalog.warehouse="$DBT_GLUE_ICEBERG_WAREHOUSE"  \
 --conf spark.sql.catalog.glue_catalog.type=glue  \
 --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO  \
 --conf spark.sql.catalog.glue_catalog.aws.region="$AWS_REGION" \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
 --conf spark.sql.warehouse.dir="$DBT_GLUE_ICEBERG_WAREHOUSE"  \
 --conf spark.hadoop.aws.glue.cache.table.enable=true \
 --conf spark.hadoop.aws.glue.cache.table.size=1000 \
 --conf spark.hadoop.aws.glue.cache.table.ttl-mins=60 \
 --conf spark.hadoop.aws.glue.cache.db.enable=true \
 --conf spark.hadoop.aws.glue.cache.db.size=1000 \
 --conf spark.hadoop.aws.glue.cache.db.ttl-mins=60 \
 --conf spark.sql.catalog.glue_catalog.cache-enabled=true \
 --conf spark.sql.catalog.spark_catalog.cache-enabled=true \
 --conf spark.sql.legacy.allowNonEmptyLocationInCTAS=true \
  >> "$THRIFT_SERVER_LOG" 2>&1 &

 #--conf spark.sql.legacy.timeParserPolicy=LEGACY \


# 'disown' ensures that once this script completes, the background process won't be killed
disown

echo "Spark Thrift Server launched in background."

# 5. Health Check Loop for Thrift Server startup
# ---------------------------------------------------
TIMEOUT=300       # Timeout after 5 minutes
WAIT_INTERVAL=5   # Check every 5 seconds
ELAPSED=0

echo "Waiting for Spark Thrift Server to start..."

# Wait until the log shows the expected string or time out
# while ! grep -q "Starting ThriftBinaryCLIService on port 10000" "$THRIFT_SERVER_LOG"; do
while ! grep -q "HiveThriftServer2: HiveThriftServer2 started" "$THRIFT_SERVER_LOG"; do
  sleep $WAIT_INTERVAL
  ELAPSED=$((ELAPSED + WAIT_INTERVAL))
  
  if [[ $ELAPSED -ge $TIMEOUT ]]; then
    echo "Error: Spark Thrift Server did not start within $TIMEOUT seconds."
    exit 1
  fi
done

echo "Spark Thrift Server is running."
exit 0
