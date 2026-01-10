# Real-time Order Processing với Kafka CDC

## 🎯 Tóm Tắt Quy Trình

**Bạn CHỈ CẦN chạy 2 file Python:**
1. `order.py` - Consumer đọc orders từ Kafka
2. `order_details.py` - Consumer đọc order_details từ Kafka

**CDC tự động chạy ngầm!** ✨

```
MySQL Database (có thay đổi data)
        ↓
   [AUTOMATIC CDC] ← Debezium connector đang chạy trong kafka-connect container
        ↓
Kafka Topics (mysql.project_db.orders, mysql.project_db.order_details)
        ↓
   [CHỈ CẦN CHẠY 2 FILE NÀY]
        ↓
  order.py + order_details.py (consumers)
        ↓
    Redis Cache → Processing
```

---

## 📋 Prerequisites

**Tất cả services này phải đang chạy (docker compose up -d):**

1. ✅ **Redis** - localhost:6379
2. ✅ **Kafka Cluster** - 2 brokers (localhost:9093, 9095)
3. ✅ **MySQL** - localhost:3306 với binlog enabled
4. ✅ **Kafka Connect** - localhost:8083 với Debezium MySQL connector

**CDC đã được setup tự động!** Không cần chạy gì thêm.

---

## 🚀 Quick Start - CHỈ 2 BƯỚC

### Bước 1: Verify CDC đang chạy

```bash
# Check Kafka Connect healthy
curl http://localhost:8083/

# Check Debezium connector status (phải là RUNNING)
curl http://localhost:8083/connectors/mysql-source-connector/status

# Check data đã có trong Kafka topics
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic mysql.project_db.orders --time -1
# Output: mysql.project_db.orders:0:25001  ← Có data rồi!
```

**Nếu connector chưa có, tạo 1 lần duy nhất:**
```bash
cd scripts/real-time
curl -X POST -H "Content-Type: application/json" \
  -d "@mysql-src-connector.json" \
  http://localhost:8083/connectors
```

### Bước 2: Chạy 2 file Python consumers

**Terminal 1 - Order Consumer:**
```bash
python scripts/real-time/order.py
```

**Terminal 2 - Order Details Consumer:**
```bash
python scripts/real-time/order_details.py
```

**XONG!** 🎉 Hệ thống đang chạy real-time processing.

---

## 📖 Chi Tiết Các Scripts

### 1. order.py - Order Consumer

**Chức năng:**
- Consume messages từ Kafka topic: `mysql.project_db.orders`
- Đọc CDC events (INSERT, UPDATE, DELETE từ MySQL orders table)
- Cache order info vào Redis với key: `order:{order_id}`
- Kiểm tra xem tất cả order_details đã ready chưa

**Auto-offset management:**
- Consumer group: `order-consumer-group`
- Auto commit offset sau khi process thành công
- Nếu crash, sẽ resume từ offset cuối cùng đã commit

**CDC Event Structure mà script nhận được:**
```python
{
    "before": None,  # None khi INSERT
    "after": {       # Data sau khi thay đổi
        "id": "uuid-xxx",
        "customer_id": 123,
        "store_id": 1,
        "payment_method_id": 2,
        "num_products": 5,
        "timestamp": 1736265600000
    },
    "op": "c",  # c=create, u=update, d=delete, r=read(snapshot)
    "source": {
        "db": "project_db",
        "table": "orders",
        "file": "mysql-bin.000001",
        "pos": 12345
    }
}
```

**Chạy:**
```bash
cd scripts/real-time
python order.py
```

**Output mẫu:**
```
Connected to Kafka: mysql.project_db.orders
Subscribed to topic(s): ['mysql.project_db.orders']
Processing message from partition 0, offset 12345
Order 550e8400-xxx: Saved to Redis
Order 550e8400-xxx: Waiting for 5 products...
```

---

### 2. order_details.py - Order Details Consumer

**Chức năng:**
- Consume messages từ Kafka topic: `mysql.project_db.order_details`
- Đọc CDC events cho order line items
- Update Redis với product info cho mỗi order
- Trigger khi đủ products cho 1 order

**Auto-offset management:**
- Consumer group: `order-details-consumer-group`
- Auto commit offset
- Resume từ last committed offset khi restart

**CDC Event Structure:**
```python
{
    "after": {
        "id": "detail-uuid-xxx",
        "order_id": "order-uuid-xxx",
        "product_id": "product-uuid",
        "quantity": 2,
        "price": 99.99
    },
    "op": "c",
    "source": {
        "db": "project_db",
        "table": "order_details"
    }
}
```

**Chạy:**
```bash
cd scripts/real-time
python order_details.py
```

**Output mẫu:**
```
Connected to Kafka: mysql.project_db.order_details
Processing order_detail for order: 550e8400-xxx
Product 1/5 received for order 550e8400-xxx
Product 5/5 received - Order 550e8400-xxx COMPLETE!
```

---

## 🔄 Quy Trình Hoạt Động Tổng Thể

### 1. CDC Background Process (TỰ ĐỘNG - Không cần can thiệp)

```
MySQL Operations (INSERT/UPDATE/DELETE vào orders hoặc order_details)
              ↓
       MySQL Binlog (ROW format)
              ↓
    Debezium Connector (kafka-connect container)
    - Đọc binlog events
    - Parse thành CDC format
    - Publish vào Kafka topics
              ↓
       Kafka Topics:
       - mysql.project_db.orders
       - mysql.project_db.order_details
```

**Debezium connector chạy 24/7 trong background!**
- Container: `kafka-connect`
- Port: 8083
- Status: Check với `curl http://localhost:8083/connectors/mysql-source-connector/status`

### 2. Consumer Processing (CHỈ CẦN CHẠY 2 FILE PYTHON)

```
Kafka Topics (có sẵn data từ CDC)
       ↓
   order.py ← Consume từ mysql.project_db.orders
       ↓
   Redis: order:{id} = {customer_id, num_products, ...}
       ↓
   Chờ order_details...
       ↓
   order_details.py ← Consume từ mysql.project_db.order_details
       ↓
   Redis: order:{id}:products = [product1, product2, ...]
       ↓
   Khi đủ products → Trigger processing
       ↓
   Complete! ✅
```

---

## 🧪 Test Real-Time CDC

### Scenario 1: Insert New Order

**Bước 1: Chạy 2 consumers (2 terminals riêng biệt)**
```bash
# Terminal 1
python scripts/real-time/order.py

# Terminal 2
python scripts/real-time/order_details.py
```

**Bước 2: Insert order mới vào MySQL**
```bash
docker exec -i mysql-db mysql -uadmin -padmin project_db -e \
  "INSERT INTO orders (id, customer_id, store_id, payment_method_id, num_products) 
   VALUES (UUID(), 123, 1, 1, 3);"
```

**Bước 3: Quan sát output**
- `order.py` sẽ ngay lập tức nhận CDC event và log
- Order được cache vào Redis
- Chờ 3 products...

**Bước 4: Insert order_details**
```bash
docker exec -i mysql-db mysql -uadmin -padmin project_db -e \
  "SET @order_id = (SELECT id FROM orders ORDER BY timestamp DESC LIMIT 1);
   INSERT INTO order_details (id, order_id, product_id, quantity, price) VALUES
   (UUID(), @order_id, UUID(), 1, 10.00),
   (UUID(), @order_id, UUID(), 2, 20.00),
   (UUID(), @order_id, UUID(), 1, 15.00);"
```

**Bước 5: Quan sát completion**
- `order_details.py` nhận 3 CDC events
- Counter: 1/3, 2/3, 3/3
- Khi đủ 3 → Trigger processing → COMPLETE!

### Scenario 2: Update Order

```bash
# Update existing order
docker exec -i mysql-db mysql -uadmin -padmin project_db -e \
  "UPDATE orders SET payment_method_id = 2 
   WHERE id = (SELECT id FROM (SELECT id FROM orders LIMIT 1) AS tmp);"
```

**CDC Event:**
```python
{
    "before": {"payment_method_id": 1, ...},  # Old value
    "after": {"payment_method_id": 2, ...},   # New value
    "op": "u"  # Update operation
}
```

`order.py` sẽ nhận event và update Redis cache.

### Scenario 3: Delete Order

```bash
docker exec -i mysql-db mysql -uadmin -padmin project_db -e \
  "DELETE FROM order_details WHERE order_id = 'some-uuid';
   DELETE FROM orders WHERE id = 'some-uuid';"
```

**CDC Event:**
```python
{
    "before": {"id": "some-uuid", ...},  # Deleted record
    "after": None,  # No data after delete
    "op": "d"  # Delete operation
}
```

Consumers có thể handle delete events để cleanup Redis.

---

## 🐛 Troubleshooting

### Issue 1: Consumers không nhận được messages

**Check 1: CDC connector có đang chạy?**
```bash
curl http://localhost:8083/connectors/mysql-source-connector/status
# Phải thấy: "state": "RUNNING"
```

**Check 2: Kafka topics có data?**
```bash
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic mysql.project_db.orders --time -1
# Phải thấy offset > 0
```

**Check 3: Consumer group có active?**
```bash
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group order-consumer-group
```

**Fix:** Restart Debezium connector
```bash
curl -X POST http://localhost:8083/connectors/mysql-source-connector/restart
```

---

### Issue 2: Consumers chạy chậm (consumer lag)

**Check lag:**
```bash
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group order-consumer-group
```

**Fix options:**
1. Increase consumers (run multiple instances)
2. Increase batch size trong Python code
3. Optimize Redis operations (pipeline, batch writes)

---

### Issue 3: Redis connection errors

**Check Redis:**
```bash
docker ps | findstr redis
docker exec redis-cache redis-cli ping
# Phải trả về: PONG
```

**Test connection:**
```bash
docker exec redis-cache redis-cli
> SET test "hello"
> GET test
> DEL test
```

---

### Issue 4: CDC không capture changes mới

**Check MySQL binlog:**
```bash
docker exec mysql-db mysql -uadmin -padmin -e "SHOW MASTER STATUS;"
# File và Position phải thay đổi sau INSERT/UPDATE
```

**Check connector offset:**
```bash
# View last processed binlog position
docker exec kafka-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic connect-offsets \
  --from-beginning --max-messages 10
```

**Fix:** Force snapshot
```bash
# Delete connector
curl -X DELETE http://localhost:8083/connectors/mysql-source-connector

# Recreate (sẽ làm snapshot lại)
curl -X POST -H "Content-Type: application/json" \
  -d "@mysql-src-connector.json" \
  http://localhost:8083/connectors
```

---

## 📊 Monitoring Commands

### Check Kafka Topics
```bash
# List all topics
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list

# Describe specific topic
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic mysql.project_db.orders
```

### View Raw Messages
```bash
# Consume from beginning
docker exec kafka-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic mysql.project_db.orders \
  --from-beginning --max-messages 5

# Consume latest messages only
docker exec kafka-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic mysql.project_db.orders \
  --offset latest --partition 0
```

### Check Redis Data
```bash
# Connect to Redis CLI
docker exec -it redis-cache redis-cli

# List all order keys
> KEYS order:*

# Get specific order
> GET order:550e8400-xxx

# Check products for order
> LRANGE order:550e8400-xxx:products 0 -1

# Count total orders in cache
> DBSIZE
```

### Monitor Consumer Groups
```bash
# List all consumer groups
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list

# Get details for specific group
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group order-consumer-group \
  --verbose
```

---

## 📝 Summary - Quy Trình Chạy

### TỰ ĐỘNG (Đã setup, chạy background):
1. ✅ MySQL binlog enabled
2. ✅ Kafka Connect với Debezium connector RUNNING
3. ✅ CDC tự động capture changes → Kafka topics
4. ✅ 25,000+ orders + 47,000+ order_details đã có trong Kafka

### CHỈ CẦN LÀM (2 commands):
```bash
# Terminal 1
python scripts/real-time/order.py

# Terminal 2  
python scripts/real-time/order_details.py
```

### Kết Quả:
- ✅ Real-time processing mọi thay đổi trong MySQL
- ✅ Auto-resume từ last offset khi restart
- ✅ Redis cache đồng bộ với database
- ✅ Scalable (chạy nhiều consumers cùng lúc)

---

**Không cần chạy gì khác!** CDC đã tự động hoạt động 24/7. 🚀
python order.py

### Start Order Details Tracker (3 workers)
```bash
python scripts\real-time\order_details.py
```

This script:
- Consumes from `mysql.project_db.order_details` topic
- Caches product IDs in Redis
- Checks if order is complete

## How It Works

1. When an order is created in MySQL, Debezium captures it and sends to Kafka topic `mysql.project_db.orders`
2. `order.py` consumes this and stores order info (customer_id, payment_method_id, num_products) in Redis
3. When order details are created, they go to `mysql.project_db.order_details` topic
4. `order_details.py` consumes and stores product IDs in Redis
5. Both scripts call `check_and_trigger()` to verify if order is complete
6. When complete, a consolidated message is sent to `order_ready_for_checking` topic

## Redis Keys Structure

```
order_info:{order_id} -> Hash
  - customer_id
  - payment_method_id
  - num_products

ordered_products:{order_id} -> Set
  - product_id_1
  - product_id_2
  - ...

order_status:{order_id} -> String
  - "checking" (when order is being processed)
```

## Monitoring

- **Kafka UI**: http://localhost:8084
- **Redis**: Use redis-cli or connect via redis client
- **Logs**: Check `logs/real-time.log`

## Troubleshooting

### Connector not running
```bash
# Restart connector
curl -X POST http://localhost:8083/connectors/mysql-source-connector/restart

# Check logs
docker logs kafka-connect
```

### No messages in Kafka
```bash
# Check topics
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list

# Check messages
docker exec kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic mysql.project_db.orders --from-beginning --max-messages 10
```

### Redis connection error
```bash
# Check Redis
docker exec redis-cache redis-cli ping

# Check Redis keys
docker exec redis-cache redis-cli --scan --pattern "order*"
```
