## =============================================================================
## MYSQL DATABASE INITIALIZATION
## =============================================================================

# Load init SQL script
docker exec -i mysql-db mysql -uadmin -padmin project_db < d:\Data\Project13-12\init-scripts\01_init.sql

# Verify tables created
docker exec mysql-db mysql -uadmin -padmin project_db -e "SHOW TABLES;"


## =============================================================================
## SPARK SETUP - Copy Python scripts to container
## =============================================================================

# Create Spark apps directory
docker exec -u root spark-master mkdir -p /opt/spark-apps

# Copy all ETL scripts to Spark container
docker cp scripts/batch/bronze_layer.py spark-master:/opt/spark-apps/
docker cp scripts/batch/silver_layer.py spark-master:/opt/spark-apps/
docker cp scripts/batch/gold_layer.py spark-master:/opt/spark-apps/

# Create logs directory with proper permissions
docker exec -u root spark-master mkdir -p /opt/logs
docker exec -u root spark-master chown -R spark:spark /opt/logs

# Verify files copied
docker exec spark-master ls -lh /opt/spark-apps/


## =============================================================================
## BRONZE-SILVER-GOLD ETL PIPELINE
## =============================================================================

# Bronze layer - Raw data ingestion from MySQL to MinIO (with partitioning)
# Dependencies: hadoop-aws (for S3/MinIO), mysql-connector-java (for JDBC)
# Output: s3a://bronze/{orders,order_details,customers,payment_method}
# Partitioning: year/month/day columns
# Full load on first run, incremental load on subsequent runs
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,mysql:mysql-connector-java:8.0.33 \
  /opt/spark-apps/bronze_layer.py

# Silver layer - Data cleaning and deduplication (Bronze -> Silver)
# Dependencies: hadoop-aws only
# Input: s3a://bronze/{tables}
# Output: s3a://silver/{tables}
# Operations: Window functions for deduplication, data quality checks
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-apps/silver_layer.py

# Gold layer - Star schema creation (Silver -> Gold)
# Dependencies: hadoop-aws only
# Input: s3a://silver/{tables}
# Output: s3a://gold/dimensions/ and s3a://gold/facts/
# Star Schema:
#   Dimensions: dim_customer, dim_payment_method, dim_product, dim_date
#   Facts: fact_orders (partitioned by order_date_id), fact_order_products
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-apps/gold_layer.py


## =============================================================================
## MONITORING & VERIFICATION
## =============================================================================

# Check Bronze layer logs
docker exec spark-master tail -n 50 /opt/logs/bronze_layer.log

# Check Silver layer logs
docker exec spark-master tail -n 50 /opt/logs/silver_layer.log

# Check Gold layer logs
docker exec spark-master tail -n 50 /opt/logs/gold_layer.log

# Access Spark UI (in browser)
# http://localhost:8080

# Check MinIO buckets (in browser)
# http://localhost:9000
# Credentials: admin / admin123

# Verify data in MinIO
docker exec minio-storage ls -lh /data/bronze/
docker exec minio-storage ls -lh /data/silver/
docker exec minio-storage ls -lh /data/gold/


## =============================================================================
## REAL-TIME CDC PROCESSING - Debezium + Kafka + Python Consumers
## =============================================================================

## ----------------------
## 1. GRANT MYSQL PRIVILEGES FOR CDC
## ----------------------

# Grant required privileges to admin user for Debezium CDC
docker exec mysql-db mysql -uroot -prootpassword -e "GRANT RELOAD, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'admin'@'%'; FLUSH PRIVILEGES;"

# Verify privileges
docker exec mysql-db mysql -uroot -prootpassword -e "SHOW GRANTS FOR 'admin'@'%';"


## ----------------------
## 2. CREATE DEBEZIUM MYSQL CDC CONNECTOR
## ----------------------

# Check existing connectors
curl http://localhost:8083/connectors

# Create MySQL CDC connector (captures changes from orders, order_details, customers, payment_method)
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "mysql-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql-db",
    "database.port": "3306",
    "database.user": "admin",
    "database.password": "admin",
    "database.server.id": "184054",
    "database.server.name": "mysql",
    "database.include.list": "project_db",
    "table.include.list": "project_db.orders,project_db.order_details,project_db.customers,project_db.payment_method",
    "database.history.kafka.bootstrap.servers": "kafka-broker:9092",
    "database.history.kafka.topic": "schema-changes.project_db",
    "snapshot.mode": "initial",
    "topic.prefix": "mysql"
  }
}'

# Check connector status (should show RUNNING with snapshotCompleted: true)
curl http://localhost:8083/connectors/mysql-connector/status

# If connector failed, restart it
curl -X POST http://localhost:8083/connectors/mysql-connector/restart


## ----------------------
## 3. VERIFY CDC TOPICS CREATED
## ----------------------

# List all Kafka topics (should see mysql.project_db.orders, mysql.project_db.order_details, etc.)
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list

# Filter for order-related topics
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list | findstr order

# Check message count in orders topic (should show ~25,000 after snapshot)
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic mysql.project_db.orders --time -1

# Check message count in order_details topic (should show ~47,089 after snapshot)
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic mysql.project_db.order_details --time -1


## ----------------------
## 4. RUN PYTHON CONSUMERS
## ----------------------

# Install Python dependencies (if not already installed)
pip install kafka-python redis

# Run order.py consumer (caches order info to Redis)
# Opens 2 workers to process mysql.project_db.orders topic
# Caches customer_id, payment_method_id, num_products to Redis db=1 with 120s TTL
cd scripts/real-time
python order.py

# (In separate terminal) Run order_details.py consumer (processes order_details and triggers completed orders)
# Opens 3 workers to process mysql.project_db.order_details topic
# Uses Redis atomic locks to prevent duplicate triggers
# Produces completed orders to order_final topic
cd scripts/real-time
python order_details.py


## ----------------------
## 5. VERIFY REAL-TIME PROCESSING
## ----------------------

# Check order_final topic created (should appear after order_details.py starts producing)
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list | findstr order_final

# Check message count in order_final topic
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic order_final --time -1

# Consume messages from order_final topic (view first 10 completed orders)
docker exec kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic order_final --from-beginning --max-messages 10

# Count total messages in order_final
docker exec kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic order_final --from-beginning --timeout-ms 5000 | Measure-Object -Line

# Check consumer group lag (should be 0 when processing complete)
docker exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group order-consumer-group
docker exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group order-details-consumer-group

