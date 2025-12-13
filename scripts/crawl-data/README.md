# Open Library Crawler

Script để crawl dữ liệu từ Open Library API và lưu vào MySQL database.

## Cấu trúc files

Files trong thư mục `scripts/crawl-data/`:

- `crawl_openlibrary.py` - Main crawler script
- `checkpoint.py` - Checkpoint management để resume crawl
- `db_helper.py` - MySQL database helper functions

File chung cho toàn project:

- `requirements.txt` - Python dependencies (ở root project)

## Chạy trực tiếp (không dùng Airflow)

```bash
# Install dependencies (requirements.txt ở root project)
pip install -r requirements.txt

# Set environment variables
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=admin
export MYSQL_PASSWORD=admin
export MYSQL_DATABASE=admin

# Run crawler
python crawl_openlibrary.py
```

## Chạy với Airflow

Script đã được tích hợp vào Airflow DAG tại `airflow/dags/openlibrary_dag.py`

### Truy cập Airflow UI
- URL: http://localhost:8082
- Username: admin
- Password: admin

### Cài đặt dependencies trong Airflow

```bash
# Exec vào container
docker exec -it airflow-webserver bash

# Install packages
pip install aiohttp mysql-connector-python

# Hoặc từ requirements.txt (nằm ở root project)
pip install -r /opt/requirements.txt
```

### Kích hoạt DAG

1. Vào Airflow UI: http://localhost:8082
2. Tìm DAG tên `openlibrary_crawler`
3. Toggle ON để enable DAG
4. Click "Trigger DAG" để chạy ngay

## Database Schema

```sql
CREATE TABLE books (
    id INT AUTO_INCREMENT PRIMARY KEY,
    work_key VARCHAR(50) UNIQUE,
    title TEXT,
    author_name TEXT,
    author_key TEXT,
    first_publish_year INT,
    isbn TEXT,
    language TEXT,
    subject TEXT,
    publisher TEXT,
    number_of_pages INT,
    cover_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Checkpoint System

- Checkpoint file: `/tmp/openlibrary_checkpoint.txt`
- Tự động lưu offset sau mỗi batch
- Resume từ offset cuối cùng khi restart

## Configuration

Trong `scripts/crawl-data/crawl_openlibrary.py`:

- `LIMIT = 100` - Records per API request
- `TOTAL_RECORDS = 50000` - Total records to crawl
- `CONCURRENCY = 5` - Concurrent requests
- `MAX_BATCH_SIZE = 500` - Insert to DB every 500 records
- `REQUEST_DELAY = 1.0` - Delay between requests (seconds)

## MySQL Connection

Environment variables:
- `MYSQL_HOST` (default: 'mysql')
- `MYSQL_PORT` (default: 3306)
- `MYSQL_USER` (default: 'admin')
- `MYSQL_PASSWORD` (default: 'admin')
- `MYSQL_DATABASE` (default: 'admin')
