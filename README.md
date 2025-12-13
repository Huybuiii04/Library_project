# Project Data Pipeline

Project này bao gồm các services:
- MySQL (Database)
- Kafka (Message Broker)
- Kafka Connect (CDC)
- Apache Spark (Data Processing)
- Apache Airflow (Workflow Orchestration)
- MinIO (Object Storage - Bronze/Silver/Gold tiers)

## Yêu cầu
- Docker
- Docker Compose

## Khởi động

```bash
# Khởi động toàn bộ services
docker-compose up -d

# Xem logs
docker-compose logs -f

# Dừng services
docker-compose down

# Dừng và xóa volumes
docker-compose down -v
```

## Truy cập Services

- **MySQL**: localhost:3306
  - User: project_user
  - Password: project_password
  - Database: project_db

- **Kafka Broker**: localhost:9093
- **Kafka Connect API**: http://localhost:8083
- **Kafka UI**: http://localhost:8084

- **MinIO Console**: http://localhost:9001
  - User: minioadmin
  - Password: minioadmin123

- **MinIO API**: http://localhost:9000
  - Buckets: bronze, silver, gold, lookup-data, order-info

- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081

- **Airflow Web UI**: http://localhost:8082
  - User: admin
  - Password: admin

## Cấu trúc thư mục

```
.
├── docker-compose.yml
├── jars/                      # JDBC drivers và JAR files
├── init-scripts/              # MySQL init scripts
├── dags/                      # Airflow DAGs
├── spark-apps/                # Spark applications
├── plugins/                   # Airflow plugins
└── logs/                      # Airflow logs
```

## Cấu hình Kafka Connect để CDC từ MySQL

```bash
# Tạo connector để capture changes từ MySQL
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "mysql-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "rootpassword",
    "database.server.id": "184054",
    "database.server.name": "mysql-server",
    "database.include.list": "project_db",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "schema-changes.project_db"
  }
}'
```

## MinIO Buckets

- **bronze**: Raw data
- **silver**: Cleaned/transformed data
- **gold**: Aggregated/business-ready data
- **lookup-data**: Reference/lookup tables
- **order-info**: Order information
