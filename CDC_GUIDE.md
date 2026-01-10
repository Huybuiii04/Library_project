# 🚀 CDC (Change Data Capture) System Guide

## Tổng Quan

System này sử dụng **Debezium** để capture thay đổi từ MySQL và stream real-time qua **Kafka**, sau đó xử lý bằng **Python consumers**.

```
MySQL Database (INSERT/UPDATE/DELETE)
        ↓
   Debezium CDC (Kafka Connect)
        ↓
Kafka Topics (mysql.project_db.orders, mysql.project_db.order_details)
        ↓
Python Consumers (order.py, order_details.py)
        ↓
    Redis Cache + order_final topic
```

---

## ✅ Prerequisites

Tất cả services phải running:
```bash
docker-compose ps
```

Required services:
- ✅ mysql-db
- ✅ kafka-broker, kafka-broker-2
- ✅ zookeeper
- ✅ kafka-connect
- ✅ redis-cache

---

## 🎯 Quick Start - Chạy CDC trong 3 bước

### Bước 1: Khởi động services
```bash
docker-compose up -d
```

### Bước 2: Tạo CDC Connector (chỉ cần 1 lần)
```bash
curl -X POST -H "Content-Type: application/json" ^
  -d "@scripts/real-time/mysql-src-connector.json" ^
  http://localhost:8083/connectors
```

Verify connector đang chạy:
```bash
curl http://localhost:8083/connectors/mysql-source-connector/status
```

Expected output:
```json
{
  "name": "mysql-source-connector",
  "connector": {"state": "RUNNING", ...},
  "tasks": [{"id": 0, "state": "RUNNING", ...}]
}
```

### Bước 3: Chạy consumers
```bash
python start_cdc_consumers.py
```

Hoặc chạy riêng từng consumer:
```bash
# Terminal 1 - Order Consumer
python scripts/real-time/order.py

# Terminal 2 - Order Details Consumer
python scripts/real-time/order_details.py
```

---

## 📊 Monitoring & Testing

### 1. Kiểm tra Kafka Topics
```bash
# List all topics
docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092

# Count messages trong topic
python scripts/real-time/check_kafka_topic.py count mysql.project_db.orders
python scripts/real-time/check_kafka_topic.py count mysql.project_db.order_details

# Xem sample data
python scripts/real-time/check_kafka_topic.py count order_final --sample
```

### 2. Kiểm tra Redis Cache
```bash
# Connect to Redis
docker exec -it redis-cache redis-cli -n 1

# Check keys
KEYS *
KEYS order_info:*
KEYS ordered_products:*

# Check specific order
HGETALL order_info:12345
SMEMBERS ordered_products:12345
```

### 3. Test CDC System
```bash
# Generate test orders
python test_cdc_system.py
```

### 4. Monitor Logs
```bash
# Consumer logs
tail -f logs/real-time.log

# Kafka Connect logs
docker logs -f kafka-connect

# MySQL binlog position
docker exec mysql-db mysql -uadmin -padmin -e "SHOW MASTER STATUS"
```

---

## 🔍 Kafka UI (Web Interface)

Truy cập: **http://localhost:8084**

Có thể xem:
- Topics và messages
- Consumer groups và lag
- Kafka Connect connectors
- Broker status

---

## 🛠️ Troubleshooting

### CDC Connector không chạy?

1. Check Kafka Connect logs:
```bash
docker logs kafka-connect
```

2. Check MySQL binlog enabled:
```bash
docker exec mysql-db mysql -uadmin -padmin -e "SHOW VARIABLES LIKE 'log_bin'"
```

3. Restart connector:
```bash
curl -X POST http://localhost:8083/connectors/mysql-source-connector/restart
```

### Consumer không nhận messages?

1. Check consumer group lag:
```bash
docker exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group order_info_tracker
```

2. Check topic có data không:
```bash
python scripts/real-time/check_kafka_topic.py count mysql.project_db.orders
```

3. Check Redis connection:
```bash
docker exec -it redis-cache redis-cli ping
```

### Reset Consumer Offset (đọc lại từ đầu)

```bash
# Stop consumers first, then:
docker exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group order_info_tracker --reset-offsets --to-earliest \
  --topic mysql.project_db.orders --execute
```

---

## 📁 File Structure

```
Project13-12/
├── start_cdc_consumers.py          # Start all consumers
├── test_cdc_system.py              # Test CDC with sample data
├── docker-compose.yml              # Infrastructure setup
├── scripts/
│   └── real-time/
│       ├── order.py                # Order consumer
│       ├── order_details.py        # Order details consumer
│       ├── kafka_handler.py        # Kafka utility class
│       ├── check_kafka_topic.py    # Kafka monitoring tool
│       └── mysql-src-connector.json # CDC connector config
└── logs/
    └── real-time.log               # Consumer logs
```

---

## 🎓 CDC Event Structure

### Orders Topic (mysql.project_db.orders)
```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 12345,
      "customer_id": 67,
      "payment_method_id": 2,
      "num_products": 3
    },
    "op": "c",  // c=create, u=update, d=delete
    "ts_ms": 1641234567890
  }
}
```

### Order Details Topic (mysql.project_db.order_details)
```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 54321,
      "order_id": 12345,
      "product_id": 789
    },
    "op": "c"
  }
}
```

---

## 🚦 System Status Check

```bash
# All-in-one health check
curl http://localhost:8083/                          # Kafka Connect
curl http://localhost:8083/connectors                # List connectors
docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092
docker exec redis-cache redis-cli ping
docker exec mysql-db mysqladmin ping -padmin
```

---

## 📈 Performance Tips

1. **Consumer Scaling**: Tăng số workers trong `order.py` và `order_details.py`
   ```python
   num_workers = 4  # Default: 2-3
   ```

2. **Kafka Partitions**: Tăng partitions cho topics
   ```bash
   docker exec kafka-broker kafka-topics --alter \
     --topic mysql.project_db.orders \
     --partitions 3 --bootstrap-server localhost:9092
   ```

3. **Redis TTL**: Điều chỉnh TTL trong code
   ```python
   redis_dynamic.expire(f"order_info:{order_id}", 300)  # 5 minutes
   ```

---

## 🔗 Useful Links

- **Kafka UI**: http://localhost:8084
- **Kafka Connect API**: http://localhost:8083
- **Airflow UI**: http://localhost:8082
- **Spark Master**: http://localhost:8080
- **MinIO Console**: http://localhost:9001

---

## 📝 Notes

- CDC connector tự động capture **INSERT, UPDATE, DELETE** từ MySQL
- Consumers sử dụng **consumer groups** để scale horizontal
- Redis cache có **TTL 2-5 minutes** để tránh memory leak
- Topic `order_final` chứa complete orders đã match đủ products

---

**Happy Streaming! 🎉**
