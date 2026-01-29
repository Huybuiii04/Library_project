# End-to-End Data Pipeline: Batch ETL & Real-Time CDC

A comprehensive data engineering project demonstrating both **batch ETL processing** (Bronze-Silver-Gold architecture) and **real-time Change Data Capture (CDC)** using Apache Spark, Kafka, Debezium, and MinIO.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Batch ETL Pipeline](#batch-etl-pipeline)
- [Real-Time CDC Pipeline](#real-time-cdc-pipeline)
- [Monitoring & Verification](#monitoring--verification)
- [Data Flow](#data-flow)
- [Troubleshooting](#troubleshooting)
- [Performance Metrics](#performance-metrics)

---

## 🎯 Overview

This project implements a production-ready data pipeline with two main components:

1. **Batch ETL Processing**: Medallion architecture (Bronze → Silver → Gold) for data warehousing
2. **Real-Time CDC**: Event-driven processing with zero-duplicate guarantee using atomic locks

### Key Features

- ✅ **Batch ETL**: Incremental data ingestion from MySQL to MinIO with Spark
- ✅ **Star Schema**: Dimensional modeling with fact and dimension tables
- ✅ **Real-Time CDC**: MySQL change capture with Debezium and Kafka
- ✅ **Zero Duplicates**: Redis atomic locks for idempotent processing
- ✅ **Scalable**: Multi-worker architecture with parallel processing
- ✅ **Monitoring**: Comprehensive logging and metrics collection

### Use Cases

- **E-commerce Order Processing**: Track orders, order details, customers, and payment methods
- **Data Warehousing**: Historical data analysis with star schema
- **Real-Time Analytics**: Immediate insights on order completion events

---

## 🏗️ Architecture

### Overall System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                     │
│                         MySQL Database                                   │
│              (orders, order_details, customers, payment_method)          │
└────────────┬────────────────────────────────────────┬───────────────────┘
             │                                        │
             │ Batch ETL                              │ Real-Time CDC
             │ (Spark JDBC)                           │ (Debezium)
             ▼                                        ▼
┌────────────────────────────┐         ┌──────────────────────────────────┐
│   BATCH ETL PIPELINE        │         │   REAL-TIME CDC PIPELINE         │
│   ────────────────────      │         │   ──────────────────────         │
│   1. Bronze Layer (Raw)     │         │   1. Debezium Connector          │
│      ├─ MySQL → MinIO       │         │      ├─ Binlog capture           │
│      ├─ Partitioned data    │         │      └─ Kafka topics             │
│      └─ Full/Incremental    │         │                                  │
│                             │         │   2. Kafka Streams               │
│   2. Silver Layer (Cleaned) │         │      ├─ mysql.project_db.orders  │
│      ├─ Deduplication       │         │      └─ mysql.project_db.        │
│      ├─ Data quality        │         │         order_details            │
│      └─ Window functions    │         │                                  │
│                             │         │   3. Python Consumers            │
│   3. Gold Layer (Curated)   │         │      ├─ order.py (2 workers)     │
│      ├─ Star schema         │         │      │   └─ Cache to Redis       │
│      ├─ Dim tables          │         │      └─ order_details.py         │
│      └─ Fact tables         │         │          (3 workers)             │
│                             │         │          └─ Atomic locks          │
└────────────┬───────────────┘         └──────────────┬───────────────────┘
             │                                        │
             ▼                                        ▼
┌────────────────────────────┐         ┌──────────────────────────────────┐
│   MinIO Object Storage      │         │   Redis Cache + Locks            │
│   ├─ bronze/                │         │   ├─ db=0: Static lookups        │
│   ├─ silver/                │         │   ├─ db=1: Order cache (TTL)     │
│   └─ gold/                  │         │   └─ db=2: Atomic locks          │
│       ├─ dimensions/        │         └──────────────────────────────────┘
│       └─ facts/             │                      │
└─────────────────────────────┘                      ▼
                                        ┌──────────────────────────────────┐
                                        │   Kafka Topic: order_final       │
                                        │   (Completed orders)             │
                                        └──────────────────────────────────┘
```

### Batch ETL - Medallion Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
│  Raw Data   │     │   Cleaned   │     │ Star Schema │
└─────────────┘     └─────────────┘     └─────────────┘
      │                   │                     │
      │                   │                     │
   MySQL              Dedup +              Dimensions:
   JDBC              Quality              - dim_customer
   Full/              Checks              - dim_payment_method
   Incr                                   - dim_product
                                          - dim_date
                                          
                                          Facts:
                                          - fact_orders
                                          - fact_order_products
```

### Real-Time CDC Flow

```
MySQL Binlog
     │
     ▼
Debezium Connector
     │
     ├─ mysql.project_db.orders ─────────────┐
     │                                        │
     └─ mysql.project_db.order_details       │
                    │                         │
                    │                         ▼
                    │                   order.py (2 workers)
                    │                         │
                    │                         ▼
                    │                   Redis Cache (db=1)
                    │                   {order_id: {customer_id,
                    │                              payment_method_id,
                    │                              num_products}}
                    ▼                         │
              order_details.py                │
              (3 workers)                     │
                    │                         │
                    ├─ Read cache ◀───────────┘
                    │
                    ├─ Verify completion
                    │
                    ├─ Atomic lock check
                    │  (Redis SET NX)
                    │
                    └─ Produce to order_final
                       (if complete & not locked)
```

---

## 🛠️ Technologies

### Core Technologies

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Data Source** | MySQL | 8.0 | Transactional database |
| **Batch Processing** | Apache Spark | 3.5.0 | Distributed data processing |
| **Stream Processing** | Apache Kafka | 7.5.0 | Event streaming platform |
| **CDC** | Debezium | 2.5 | Change Data Capture |
| **Object Storage** | MinIO | Latest | S3-compatible storage |
| **Cache & Locks** | Redis | Latest | In-memory data store |
| **Orchestration** | Docker Compose | - | Container orchestration |
| **Language** | Python | 3.x | Consumer applications |

### Python Dependencies

```
pyspark==3.5.0
kafka-python==2.0.2
redis==5.0.0
mysql-connector-python==8.0.33
```

### Infrastructure Components

- **Zookeeper**: Kafka coordination
- **Kafka Connect**: Debezium connector hosting
- **Spark Master & Workers**: Distributed processing
- **Airflow** (Optional): Workflow orchestration

---

## ✅ Prerequisites

### System Requirements

- **OS**: Windows 10/11, Linux, or macOS
- **RAM**: Minimum 8GB (16GB recommended)
- **CPU**: 4 cores minimum (8 cores recommended)
- **Disk**: 20GB free space
- **Docker**: Version 20.10+
- **Docker Compose**: Version 2.0+

### Software Installation

1. **Docker Desktop**
   ```bash
   # Download from https://www.docker.com/products/docker-desktop
   # Verify installation
   docker --version
   docker-compose --version
   ```

2. **Python 3.x**
   ```bash
   # Verify installation
   python --version
   pip --version
   ```

3. **Git** (optional, for cloning)
   ```bash
   git --version
   ```

---

## 📁 Project Structure

```
Project13-12/
├── docker-compose.yml              # Container orchestration
├── requirements.txt                # Python dependencies
├── README.md                       # This file
├── QUICKSTART.md                  # Quick start guide
│
├── init-scripts/                   # Database initialization
│   ├── 01_init.sql                # MySQL schema and seed data
│   └── run.txt                    # Complete command reference
│
├── data/                          # CSV data files
│   ├── customers.csv              # Customer master data
│   └── payment_method.csv         # Payment method lookup
│
├── scripts/
│   ├── batch/                     # Batch ETL scripts
│   │   ├── bronze_layer.py        # Raw data ingestion (MySQL → MinIO)
│   │   ├── silver_layer.py        # Data cleaning & deduplication
│   │   └── gold_layer.py          # Star schema creation
│   │
│   ├── real-time/                 # Real-time CDC scripts
│   │   ├── kafka_handler.py       # Kafka producer/consumer wrapper
│   │   ├── order.py               # Order info caching (2 workers)
│   │   ├── order_details.py       # Order completion processing (3 workers)
│   │   ├── mysql-src-connector.json  # Debezium connector config
│   │   └── QUICKSTART.md          # Real-time quick start
│   │
│   └── database/                  # Database utilities
│       ├── generate_orders.py     # Generate sample orders
│       └── load_file.py           # Load CSV to MySQL
│
├── logs/                          # Application logs
│   ├── bronze_layer.log
│   ├── silver_layer.log
│   ├── gold_layer.log
│   └── real-time.log
│
└── jars/                          # Java dependencies
    └── debezium/                  # Debezium connector JARs
```

---

## 🚀 Setup & Installation

### Step 1: Clone Repository (or create project structure)

```bash
# If cloning from repository
git clone <repository-url>
cd Project13-12

# Or create manually
mkdir Project13-12
cd Project13-12
```

### Step 2: Start Docker Containers

```bash
# Start all services
docker-compose up -d

# Verify all containers are running
docker ps

# Expected containers:
# - mysql-db
# - kafka-broker
# - zookeeper
# - kafka-connect
# - spark-master
# - spark-worker-1
# - spark-worker-2
# - minio-storage
# - redis-cache
```

### Step 3: Initialize MySQL Database

```bash
# Load initial schema and data
docker exec -i mysql-db mysql -uadmin -padmin project_db < init-scripts/01_init.sql

# Verify tables created
docker exec mysql-db mysql -uadmin -padmin project_db -e "SHOW TABLES;"

# Expected output:
# +----------------------+
# | Tables_in_project_db |
# +----------------------+
# | customers            |
# | order_details        |
# | orders               |
# | payment_method       |
# +----------------------+
```

### Step 4: Install Python Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 5: Grant MySQL Privileges for CDC

```bash
# Required for Debezium CDC
docker exec mysql-db mysql -uroot -prootpassword -e "GRANT RELOAD, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'admin'@'%'; FLUSH PRIVILEGES;"

# Verify privileges
docker exec mysql-db mysql -uroot -prootpassword -e "SHOW GRANTS FOR 'admin'@'%';"
```

---

## 📊 Batch ETL Pipeline

### Overview

The batch ETL pipeline implements a **Medallion Architecture** with three layers:

1. **Bronze Layer**: Raw data ingestion from MySQL to MinIO
2. **Silver Layer**: Data cleaning, deduplication, and quality checks
3. **Gold Layer**: Star schema with dimension and fact tables

### Copy Scripts to Spark Container

```bash
# Create directory
docker exec -u root spark-master mkdir -p /opt/spark-apps

# Copy ETL scripts
docker cp scripts/batch/bronze_layer.py spark-master:/opt/spark-apps/
docker cp scripts/batch/silver_layer.py spark-master:/opt/spark-apps/
docker cp scripts/batch/gold_layer.py spark-master:/opt/spark-apps/

# Create logs directory
docker exec -u root spark-master mkdir -p /opt/logs
docker exec -u root spark-master chown -R spark:spark /opt/logs
```

### Run Bronze Layer

```bash
# Ingest raw data from MySQL to MinIO (s3a://bronze/)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,mysql:mysql-connector-java:8.0.33 \
  /opt/spark-apps/bronze_layer.py
```

**Output**:
- `s3a://bronze/orders/` - 25,000 records
- `s3a://bronze/order_details/` - 47,089 records
- `s3a://bronze/customers/` - 1,000,000 records
- `s3a://bronze/payment_method/` - 12 records

**Features**:
- Partitioning by year/month/day
- Full load on first run
- Incremental load on subsequent runs (based on timestamp)

### Run Silver Layer

```bash
# Clean and deduplicate data (Bronze → Silver)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-apps/silver_layer.py
```

**Output**:
- `s3a://silver/orders/` - Deduplicated orders
- `s3a://silver/order_details/` - Deduplicated order details
- `s3a://silver/customers/` - Cleaned customer data
- `s3a://silver/payment_method/` - Validated payment methods

**Operations**:
- Window functions for deduplication
- Data quality validation
- Null handling
- Type conversions

### Run Gold Layer

```bash
# Create star schema (Silver → Gold)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-apps/gold_layer.py
```

**Output - Dimension Tables**:
- `s3a://gold/dimensions/dim_customer/` - Customer dimension
- `s3a://gold/dimensions/dim_payment_method/` - Payment method dimension
- `s3a://gold/dimensions/dim_product/` - Product dimension
- `s3a://gold/dimensions/dim_date/` - Date dimension

**Output - Fact Tables**:
- `s3a://gold/facts/fact_orders/` - Order facts (partitioned by order_date_id)
- `s3a://gold/facts/fact_order_products/` - Order-product relationships

**Star Schema Benefits**:
- Optimized for analytical queries
- Denormalized for performance
- Partitioned for efficient scans
- SCD Type 1 dimensions

---

## ⚡ Real-Time CDC Pipeline

### Overview

The real-time CDC pipeline captures MySQL changes using **Debezium** and processes them with **Python consumers** to achieve:

- **Zero-duplicate guarantee** using Redis atomic locks
- **Exactly-once semantics** for order completion events
- **Multi-worker parallel processing** for high throughput

### Architecture Components

1. **Debezium MySQL Connector**: Captures binlog changes
2. **Kafka Topics**: Streams CDC events
3. **order.py**: Caches order metadata to Redis (2 workers)
4. **order_details.py**: Processes order details and triggers completion (3 workers)
5. **Redis**: Provides cache and atomic locks

### Step 1: Create Debezium Connector

```bash
# Check existing connectors
curl http://localhost:8083/connectors

# Create MySQL CDC connector
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

# Check connector status (wait for snapshotCompleted: true)
curl http://localhost:8083/connectors/mysql-connector/status
```

### Step 2: Verify CDC Topics

```bash
# List all Kafka topics
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list

# Expected topics:
# - mysql.project_db.orders
# - mysql.project_db.order_details
# - mysql.project_db.customers
# - mysql.project_db.payment_method

# Check message counts
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic mysql.project_db.orders \
  --time -1

# Expected: mysql.project_db.orders:0:25000
```

### Step 3: Run Python Consumers

#### Terminal 1: Start order.py

```bash
cd scripts/real-time
python order.py
```

**What it does**:
- Consumes from `mysql.project_db.orders` topic
- Caches order metadata to Redis db=1:
  - Key: `order:{order_id}`
  - Value: `{customer_id, payment_method_id, num_products}`
  - TTL: 120 seconds
- Runs 2 parallel workers
- Consumer group: `order-consumer-group`

**Log output**:
```
2026-01-10 22:24:49 - INFO - order - [ORDER_CACHED] fff89e2e-2ed8-41d1-a9ac-958b4e98e936 - customer: 999034, products: 4
```

#### Terminal 2: Start order_details.py

```bash
cd scripts/real-time
python order_details.py
```

**What it does**:
- Consumes from `mysql.project_db.order_details` topic
- Retrieves cached order info from Redis
- Verifies order completion (all products received)
- Uses Redis atomic locks to prevent duplicates:
  - Lock key: `order_trigger_lock:{order_id}`
  - TTL: 10 seconds
  - Mechanism: `SET NX` (set if not exists)
- Produces completed orders to `order_final` topic
- Runs 3 parallel workers
- Consumer group: `order-details-consumer-group`

**Log output**:
```
2026-01-10 22:30:15 - INFO - order_details - [ORDER_TRIGGERED] abc123... - Products: 3, Customer: 12345
```

### Step 4: Verify Processing

```bash
# Check order_final topic created
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list | findstr order_final

# Check message count
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic order_final \
  --time -1

# Consume messages
docker exec kafka-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order_final \
  --from-beginning \
  --max-messages 10

# Count total messages (PowerShell)
docker exec kafka-broker kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic order_final `
  --from-beginning `
  --timeout-ms 5000 | Measure-Object -Line

# Expected: ~25,000 completed orders
```

### Step 5: Test Real-Time CDC

Insert a new order and verify it's processed in real-time:

```bash
# 1. Insert new order
docker exec mysql-db mysql -uadmin -padmin project_db -e "
INSERT INTO orders (id, customer_id, store_id, payment_method_id, num_products, timestamp)
VALUES (UUID(), 99999, 1, 1, 2, NOW());
"

# 2. Get the order_id
docker exec mysql-db mysql -uadmin -padmin project_db -e "
SELECT id, customer_id, num_products FROM orders 
WHERE customer_id = 99999 ORDER BY timestamp DESC LIMIT 1;
"

# 3. Insert order_details (replace YOUR_ORDER_ID)
docker exec mysql-db mysql -uadmin -padmin project_db -e "
INSERT INTO order_details (order_id, product_id, quantity)
VALUES ('YOUR_ORDER_ID', 1001, 1), ('YOUR_ORDER_ID', 1002, 1);
"

# 4. Verify in order_final topic (should appear within 1-2 seconds)
docker exec kafka-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order_final \
  --from-beginning \
  --max-messages 1000 | findstr YOUR_ORDER_ID
```

---

## 📈 Monitoring & Verification

### Batch ETL Monitoring

#### Check Spark Logs

```bash
# Bronze layer logs
docker exec spark-master tail -n 50 /opt/logs/bronze_layer.log

# Silver layer logs
docker exec spark-master tail -n 50 /opt/logs/silver_layer.log

# Gold layer logs
docker exec spark-master tail -n 50 /opt/logs/gold_layer.log
```

#### Access Web UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Spark Master** | http://localhost:8080 | - |
| **Spark Worker 1** | http://localhost:8081 | - |
| **Spark Worker 2** | http://localhost:8082 | - |
| **MinIO Console** | http://localhost:9000 | admin / admin123 |

#### Verify MinIO Data

```bash
# List buckets
docker exec minio-storage ls -lh /data/

# Check Bronze data
docker exec minio-storage ls -lh /data/bronze/orders/

# Check Silver data
docker exec minio-storage ls -lh /data/silver/orders/

# Check Gold dimensions
docker exec minio-storage ls -lh /data/gold/dimensions/

# Check Gold facts
docker exec minio-storage ls -lh /data/gold/facts/
```

### Real-Time CDC Monitoring

#### Check Debezium Connector

```bash
# Connector status
curl http://localhost:8083/connectors/mysql-connector/status

# Expected output:
# {
#   "name": "mysql-connector",
#   "connector": {
#     "state": "RUNNING",
#     "worker_id": "kafka-connect:8083"
#   },
#   "tasks": [
#     {
#       "id": 0,
#       "state": "RUNNING",
#       "worker_id": "kafka-connect:8083"
#     }
#   ],
#   "type": "source"
# }

# Connector configuration
curl http://localhost:8083/connectors/mysql-connector

# Delete connector (if needed)
curl -X DELETE http://localhost:8083/connectors/mysql-connector
```

#### Check Consumer Lag

```bash
# order.py consumer group
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-consumer-group

# order_details.py consumer group
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-details-consumer-group

# Expected: LAG = 0 when all messages are consumed
```

#### Check Redis Cache

```bash
# Count cached orders in Redis db=1
docker exec redis-cache redis-cli -n 1 KEYS "order:*" | wc -l

# View specific cached order
docker exec redis-cache redis-cli -n 1 GET "order:YOUR_ORDER_ID"

# Check atomic locks in Redis db=2
docker exec redis-cache redis-cli -n 2 KEYS "order_trigger_lock:*"

# Expected: Empty or locks with short TTL (~10 seconds)
```

#### Check Kafka Topics

```bash
# List all topics
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list

# Describe topic details
docker exec kafka-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic order_final

# Check topic message count
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic order_final \
  --time -1
```

#### Application Logs

```bash
# Real-time processing logs
tail -f logs/real-time.log

# Look for:
# - [ORDER_CACHED] - order.py cached order
# - [ORDER_TRIGGERED] - order_details.py triggered completion
# - [DUPLICATE_PREVENTED] - atomic lock prevented duplicate
```

---

## 🔄 Data Flow

### Batch ETL Data Flow

```
1. MySQL → Bronze Layer
   ├─ JDBC connection to MySQL
   ├─ Read orders, order_details, customers, payment_method
   ├─ Add partitioning columns (year, month, day)
   └─ Write to MinIO: s3a://bronze/{table}/year={y}/month={m}/day={d}/

2. Bronze → Silver Layer
   ├─ Read from s3a://bronze/
   ├─ Deduplicate using window functions
   ├─ Data quality checks (null handling, type validation)
   └─ Write to MinIO: s3a://silver/{table}/

3. Silver → Gold Layer
   ├─ Read from s3a://silver/
   ├─ Create dimension tables (SCD Type 1)
   │   ├─ dim_customer (customer_id as SK)
   │   ├─ dim_payment_method (payment_method_id as SK)
   │   ├─ dim_product (product_id as SK)
   │   └─ dim_date (date_id as SK in YYYYMMDD format)
   ├─ Create fact tables with foreign keys
   │   ├─ fact_orders (order_id, customer_id, payment_method_id, order_date_id)
   │   └─ fact_order_products (order_id, product_id, quantity)
   └─ Write to MinIO: s3a://gold/{dimensions,facts}/
```

### Real-Time CDC Data Flow

```
1. MySQL Binlog Changes
   ├─ INSERT/UPDATE/DELETE operations
   └─ Captured by Debezium connector

2. Debezium → Kafka
   ├─ mysql.project_db.orders → Order insert/update events
   └─ mysql.project_db.order_details → Order detail insert events

3. Kafka → order.py (2 workers)
   ├─ Consume from mysql.project_db.orders
   ├─ Parse CDC event (after.id, after.customer_id, after.num_products)
   └─ Cache to Redis db=1:
       Key: order:{order_id}
       Value: {customer_id, payment_method_id, num_products}
       TTL: 120 seconds

4. Kafka → order_details.py (3 workers)
   ├─ Consume from mysql.project_db.order_details
   ├─ Parse CDC event (after.order_id, after.product_id)
   ├─ Lookup cached order from Redis db=1
   ├─ Check order completion:
   │   - Count received products
   │   - Compare with expected num_products
   ├─ If complete:
   │   ├─ Try atomic lock: SET NX order_trigger_lock:{order_id} with 10s TTL
   │   ├─ If lock acquired:
   │   │   ├─ Produce to order_final topic
   │   │   └─ Log [ORDER_TRIGGERED]
   │   └─ If lock exists:
   │       └─ Log [DUPLICATE_PREVENTED] (another worker already triggered)
   └─ If incomplete:
       └─ Log [PRODUCTS_INCOMPLETE]

5. order_final Topic
   ├─ Contains only completed orders
   ├─ Guaranteed zero duplicates
   └─ Ready for downstream processing (analytics, notifications, etc.)
```

---

## 🔧 Troubleshooting

### Common Issues

#### 1. Docker Containers Not Starting

**Symptoms**: Services fail health checks or exit immediately

**Solutions**:
```bash
# Check container logs
docker logs mysql-db
docker logs kafka-broker
docker logs spark-master

# Verify ports are not in use
netstat -an | findstr "3306 9092 8080 9000"

# Restart specific service
docker-compose restart mysql-db

# Restart all services
docker-compose down
docker-compose up -d
```

#### 2. Spark Job Fails - Out of Memory

**Symptoms**: `java.lang.OutOfMemoryError`

**Solutions**:
```python
# Increase executor memory in Python scripts
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \  # Increase from 2g to 4g
    .config("spark.driver.memory", "4g") \    # Increase driver memory
    .getOrCreate()
```

Or in spark-submit:
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --executor-memory 4g \
  --driver-memory 4g \
  --master spark://spark-master:7077 \
  /opt/spark-apps/bronze_layer.py
```

#### 3. Debezium Connector Failed

**Symptoms**: Connector status shows `FAILED`

**Check logs**:
```bash
docker logs kafka-connect
```

**Common causes and solutions**:

a) **Missing MySQL privileges**:
```bash
docker exec mysql-db mysql -uroot -prootpassword -e "
GRANT RELOAD, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'admin'@'%';
FLUSH PRIVILEGES;"
```

b) **Connector already exists**:
```bash
# Delete and recreate
curl -X DELETE http://localhost:8083/connectors/mysql-connector
# Then create again with POST
```

c) **Kafka Connect not ready**:
```bash
# Wait for Kafka Connect to start
curl http://localhost:8083/
# Should return: {"version":"7.5.0","commit":"..."}
```

#### 4. No Messages in order_final Topic

**Symptoms**: `order_final` topic is empty or has fewer messages than expected

**Check order.py is running**:
```bash
# order.py must run before order_details.py
# Verify logs show [ORDER_CACHED] messages
```

**Check Redis cache**:
```bash
# Verify orders are cached
docker exec redis-cache redis-cli -n 1 KEYS "order:*" | wc -l
# Should show cached orders (may be 0 if all TTL expired)
```

**Check consumer lag**:
```bash
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-details-consumer-group
```

**Check atomic locks**:
```bash
# If all orders already triggered, locks should be expired
docker exec redis-cache redis-cli -n 2 KEYS "order_trigger_lock:*"
```

#### 5. Duplicate Orders in order_final

**Symptoms**: Same order_id appears multiple times in `order_final` topic

**This should NOT happen** due to atomic locks. If it does:

**Verify atomic lock implementation**:
```python
# In order_details.py, check this code:
lock_key = f"order_trigger_lock:{order_id}"
if not redis_locks.set(lock_key, "1", nx=True, ex=10):
    logger.warning(f"[DUPLICATE_PREVENTED] {order_id} - Lock exists")
    return
```

**Check Redis is accessible**:
```bash
docker exec redis-cache redis-cli ping
# Should return: PONG
```

#### 6. MinIO Access Denied

**Symptoms**: `AccessDeniedException` or `403 Forbidden` when accessing MinIO

**Verify credentials**:
```python
# In Spark scripts, check:
.config("spark.hadoop.fs.s3a.access.key", "admin")
.config("spark.hadoop.fs.s3a.secret.key", "admin123")
```

**Create buckets manually**:
```bash
# Access MinIO console: http://localhost:9000
# Login: admin / admin123
# Create buckets: bronze, silver, gold
```

#### 7. Kafka Consumer Not Receiving Messages

**Symptoms**: Consumer starts but no messages are processed

**Check topic exists**:
```bash
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list
```

**Check message count**:
```bash
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic mysql.project_db.orders \
  --time -1
```

**Check consumer group**:
```bash
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-consumer-group
```

**Reset consumer offset** (if needed):
```bash
# WARNING: This will reprocess all messages
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group order-consumer-group \
  --reset-offsets \
  --to-earliest \
  --topic mysql.project_db.orders \
  --execute
```

---



