"""
Bronze-Silver-Gold Batch Processing with Star Schema
Reads from MySQL -> Bronze -> Silver -> Gold (Star Schema)

Architecture:
- Bronze: Raw data from MySQL (orders, order_details, customers, payment_method)
- Silver: Cleaned and deduplicated data
- Gold: Star Schema (fact_orders + dimensions)
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Add parent directory to path for utils import
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

# Setup logging
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "batch_star_schema.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session with MinIO S3 configuration"""
    return SparkSession.builder \
        .appName("Bronze-Silver-Gold Star Schema ETL") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir", "s3a://gold-layer/warehouse") \
        .getOrCreate()


def read_mysql_table(spark: SparkSession, table: str):
    """Read table from MySQL source database"""
    host = os.getenv("MYSQL_HOST", "mysql")
    user = os.getenv("MYSQL_USER", "admin")
    password = os.getenv("MYSQL_PASSWORD", "admin")
    database = os.getenv("MYSQL_DATABASE", "project_db")
    
    logger.info(f"[MYSQL] Reading table: {table}")
    
    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{host}:3306/{database}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", table) \
        .load()


# =====================================================================
# BRONZE LAYER - Raw data ingestion from MySQL
# =====================================================================

def check_bronze_data_exists(spark: SparkSession, path: str) -> bool:
    """Check if Bronze layer already has data"""
    try:
        spark.read.parquet(path)
        return True
    except:
        return False


def bronze_ingest_orders(spark: SparkSession):
    """Ingest orders table to Bronze layer with incremental load support"""
    logger.info("=== [BRONZE] Ingesting orders ===")
    
    output_path = "s3a://bronze-layer/orders"
    
    # Read from MySQL
    orders_df = read_mysql_table(spark, "orders")
    
    # Check if Bronze data exists
    if check_bronze_data_exists(spark, output_path):
        logger.info("[BRONZE][orders] Existing data found -> Incremental load")
        
        # Read existing Bronze data
        existing_df = spark.read.parquet(output_path)
        
        # Find latest timestamp (assuming 'created_at' or 'timestamp' column exists)
        # Try different timestamp column names
        timestamp_col = None
        for col_name in ["timestamp", "created_at", "updated_at"]:
            if col_name in orders_df.columns:
                timestamp_col = col_name
                break
        
        if timestamp_col:
            max_timestamp = existing_df.select(max(col(timestamp_col))).first()[0]
            logger.info(f"[BRONZE][orders] Latest timestamp in Bronze: {max_timestamp}")
            
            # Filter only new records
            new_orders = orders_df.filter(col(timestamp_col) > max_timestamp)
            new_count = new_orders.count()
            
            if new_count == 0:
                logger.info("[BRONZE][orders] No new records to ingest")
                return existing_df
            
            logger.info(f"[BRONZE][orders] Found {new_count} new records")
            orders_to_process = new_orders
        else:
            logger.warning("[BRONZE][orders] No timestamp column found, doing full load")
            orders_to_process = orders_df
    else:
        logger.info("[BRONZE][orders] No existing data -> Full load")
        orders_to_process = orders_df
    
    # Add ingestion metadata and partitioning columns
    bronze_orders = orders_to_process \
        .withColumn("bronze_ingestion_time", current_timestamp()) \
        .withColumn("bronze_source", lit("mysql.project_db.orders"))
    
    # Add partitioning columns based on created_at or current time
    if "created_at" in bronze_orders.columns:
        bronze_orders = bronze_orders \
            .withColumn("year", year(col("created_at"))) \
            .withColumn("month", month(col("created_at"))) \
            .withColumn("day", dayofmonth(col("created_at")))
    elif "timestamp" in bronze_orders.columns:
        bronze_orders = bronze_orders \
            .withColumn("year", year(col("timestamp"))) \
            .withColumn("month", month(col("timestamp"))) \
            .withColumn("day", dayofmonth(col("timestamp")))
    else:
        bronze_orders = bronze_orders \
            .withColumn("year", year(current_timestamp())) \
            .withColumn("month", month(current_timestamp())) \
            .withColumn("day", dayofmonth(current_timestamp()))
    
    # Write to Bronze layer (append mode for incremental)
    write_mode = "append" if check_bronze_data_exists(spark, output_path) else "overwrite"
    
    bronze_orders.write \
        .mode(write_mode) \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)
    
    count = bronze_orders.count()
    logger.info(f"[BRONZE] Orders ingested: {count} records -> {output_path}")
    
    return bronze_orders


def bronze_ingest_order_details(spark: SparkSession):
    """Ingest order_details table to Bronze layer with incremental load support"""
    logger.info("=== [BRONZE] Ingesting order_details ===")
    
    output_path = "s3a://bronze-layer/order_details"
    
    # Read from MySQL
    order_details_df = read_mysql_table(spark, "order_details")
    
    # Check if Bronze data exists
    if check_bronze_data_exists(spark, output_path):
        logger.info("[BRONZE][order_details] Existing data found -> Incremental load")
        
        existing_df = spark.read.parquet(output_path)
        
        # Try to find timestamp column
        timestamp_col = None
        for col_name in ["timestamp", "created_at", "updated_at"]:
            if col_name in order_details_df.columns:
                timestamp_col = col_name
                break
        
        if timestamp_col:
            max_timestamp = existing_df.select(max(col(timestamp_col))).first()[0]
            logger.info(f"[BRONZE][order_details] Latest timestamp: {max_timestamp}")
            
            new_details = order_details_df.filter(col(timestamp_col) > max_timestamp)
            new_count = new_details.count()
            
            if new_count == 0:
                logger.info("[BRONZE][order_details] No new records to ingest")
                return existing_df
            
            logger.info(f"[BRONZE][order_details] Found {new_count} new records")
            details_to_process = new_details
        else:
            logger.warning("[BRONZE][order_details] No timestamp column, doing full load")
            details_to_process = order_details_df
    else:
        logger.info("[BRONZE][order_details] No existing data -> Full load")
        details_to_process = order_details_df
    
    # Add metadata and partitioning
    bronze_order_details = details_to_process \
        .withColumn("bronze_ingestion_time", current_timestamp()) \
        .withColumn("bronze_source", lit("mysql.project_db.order_details"))
    
    # Add partitioning columns
    if "created_at" in bronze_order_details.columns:
        bronze_order_details = bronze_order_details \
            .withColumn("year", year(col("created_at"))) \
            .withColumn("month", month(col("created_at"))) \
            .withColumn("day", dayofmonth(col("created_at")))
    elif "timestamp" in bronze_order_details.columns:
        bronze_order_details = bronze_order_details \
            .withColumn("year", year(col("timestamp"))) \
            .withColumn("month", month(col("timestamp"))) \
            .withColumn("day", dayofmonth(col("timestamp")))
    else:
        bronze_order_details = bronze_order_details \
            .withColumn("year", year(current_timestamp())) \
            .withColumn("month", month(current_timestamp())) \
            .withColumn("day", dayofmonth(current_timestamp()))
    
    write_mode = "append" if check_bronze_data_exists(spark, output_path) else "overwrite"
    
    bronze_order_details.write \
        .mode(write_mode) \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)
    
    count = bronze_order_details.count()
    logger.info(f"[BRONZE] Order details ingested: {count} records -> {output_path}")
    
    return bronze_order_details


def bronze_ingest_customers(spark: SparkSession):
    """Ingest customers table to Bronze layer with incremental load support"""
    logger.info("=== [BRONZE] Ingesting customers ===")
    
    output_path = "s3a://bronze-layer/customers"
    
    # Read from MySQL
    customers_df = read_mysql_table(spark, "customers")
    
    # Check if Bronze data exists
    if check_bronze_data_exists(spark, output_path):
        logger.info("[BRONZE][customers] Existing data found -> Incremental load")
        
        existing_df = spark.read.parquet(output_path)
        
        # For dimension tables like customers, we can use customer_id for deduplication
        # Or use timestamp if available
        timestamp_col = None
        for col_name in ["timestamp", "created_at", "updated_at"]:
            if col_name in customers_df.columns:
                timestamp_col = col_name
                break
        
        if timestamp_col:
            max_timestamp = existing_df.select(max(col(timestamp_col))).first()[0]
            logger.info(f"[BRONZE][customers] Latest timestamp: {max_timestamp}")
            
            new_customers = customers_df.filter(col(timestamp_col) > max_timestamp)
            new_count = new_customers.count()
            
            if new_count == 0:
                logger.info("[BRONZE][customers] No new records to ingest")
                return existing_df
            
            logger.info(f"[BRONZE][customers] Found {new_count} new records")
            customers_to_process = new_customers
        else:
            logger.warning("[BRONZE][customers] No timestamp column, doing full load")
            customers_to_process = customers_df
    else:
        logger.info("[BRONZE][customers] No existing data -> Full load")
        customers_to_process = customers_df
    
    # Add metadata and partitioning
    bronze_customers = customers_to_process \
        .withColumn("bronze_ingestion_time", current_timestamp()) \
        .withColumn("bronze_source", lit("mysql.project_db.customers"))
    
    # Add partitioning columns
    if "created_at" in bronze_customers.columns:
        bronze_customers = bronze_customers \
            .withColumn("year", year(col("created_at"))) \
            .withColumn("month", month(col("created_at"))) \
            .withColumn("day", dayofmonth(col("created_at")))
    else:
        bronze_customers = bronze_customers \
            .withColumn("year", year(current_timestamp())) \
            .withColumn("month", month(current_timestamp())) \
            .withColumn("day", dayofmonth(current_timestamp()))
    
    write_mode = "append" if check_bronze_data_exists(spark, output_path) else "overwrite"
    
    bronze_customers.write \
        .mode(write_mode) \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)
    
    count = bronze_customers.count()
    logger.info(f"[BRONZE] Customers ingested: {count} records -> {output_path}")
    
    return bronze_customers


def bronze_ingest_payment_method(spark: SparkSession):
    """Ingest payment_method table to Bronze layer with incremental load support"""
    logger.info("=== [BRONZE] Ingesting payment_method ===")
    
    output_path = "s3a://bronze-layer/payment_method"
    
    # Read from MySQL
    payment_df = read_mysql_table(spark, "payment_method")
    
    # Check if Bronze data exists
    if check_bronze_data_exists(spark, output_path):
        logger.info("[BRONZE][payment_method] Existing data found -> Incremental load")
        
        existing_df = spark.read.parquet(output_path)
        
        # Payment method is usually a small static table, but check for updates
        timestamp_col = None
        for col_name in ["timestamp", "created_at", "updated_at"]:
            if col_name in payment_df.columns:
                timestamp_col = col_name
                break
        
        if timestamp_col:
            max_timestamp = existing_df.select(max(col(timestamp_col))).first()[0]
            logger.info(f"[BRONZE][payment_method] Latest timestamp: {max_timestamp}")
            
            new_payment = payment_df.filter(col(timestamp_col) > max_timestamp)
            new_count = new_payment.count()
            
            if new_count == 0:
                logger.info("[BRONZE][payment_method] No new records to ingest")
                return existing_df
            
            logger.info(f"[BRONZE][payment_method] Found {new_count} new records")
            payment_to_process = new_payment
        else:
            logger.warning("[BRONZE][payment_method] No timestamp column, doing full load")
            payment_to_process = payment_df
    else:
        logger.info("[BRONZE][payment_method] No existing data -> Full load")
        payment_to_process = payment_df
    
    # Add metadata and partitioning
    bronze_payment = payment_to_process \
        .withColumn("bronze_ingestion_time", current_timestamp()) \
        .withColumn("bronze_source", lit("mysql.project_db.payment_method"))
    
    # Add partitioning columns
    if "created_at" in bronze_payment.columns:
        bronze_payment = bronze_payment \
            .withColumn("year", year(col("created_at"))) \
            .withColumn("month", month(col("created_at"))) \
            .withColumn("day", dayofmonth(col("created_at")))
    else:
        bronze_payment = bronze_payment \
            .withColumn("year", year(current_timestamp())) \
            .withColumn("month", month(current_timestamp())) \
            .withColumn("day", dayofmonth(current_timestamp()))
    
    write_mode = "append" if check_bronze_data_exists(spark, output_path) else "overwrite"
    
    bronze_payment.write \
        .mode(write_mode) \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)
    
    count = bronze_payment.count()
    logger.info(f"[BRONZE] Payment methods ingested: {count} records -> {output_path}")
    
    return bronze_payment



# =====================================================================
# SILVER LAYER - Cleaned and transformed data
# =====================================================================

def silver_clean_orders(spark: SparkSession):
    """Clean and deduplicate orders for Silver layer"""
    logger.info("=== [SILVER] Cleaning orders ===")
    
    bronze_orders = spark.read.parquet("s3a://bronze-layer/orders")
    
    # Deduplication based on order id (keep most recent)
    window_spec = Window.partitionBy("id").orderBy(col("bronze_ingestion_time").desc())
    
    silver_orders = bronze_orders \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num") \
        .withColumn("silver_processed_time", current_timestamp()) \
        .withColumnRenamed("id", "order_id") \
        .withColumnRenamed("created_at", "order_created_at")
    
    output_path = "s3a://silver-layer/orders"
    
    silver_orders.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = silver_orders.count()
    logger.info(f"[SILVER] Orders cleaned: {count} records -> {output_path}")
    
    return silver_orders


def silver_clean_order_details(spark: SparkSession):
    """Clean order_details for Silver layer"""
    logger.info("=== [SILVER] Cleaning order_details ===")
    
    bronze_order_details = spark.read.parquet("s3a://bronze-layer/order_details")
    
    # Deduplication
    window_spec = Window.partitionBy("order_id", "product_id").orderBy(col("bronze_ingestion_time").desc())
    
    silver_order_details = bronze_order_details \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num") \
        .withColumn("silver_processed_time", current_timestamp()) \
        .withColumn("quantity", lit(1))  # Default quantity
    
    output_path = "s3a://silver-layer/order_details"
    
    silver_order_details.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = silver_order_details.count()
    logger.info(f"[SILVER] Order details cleaned: {count} records -> {output_path}")
    
    return silver_order_details


def silver_clean_customers(spark: SparkSession):
    """Clean customers for Silver layer"""
    logger.info("=== [SILVER] Cleaning customers ===")
    
    bronze_customers = spark.read.parquet("s3a://bronze-layer/customers")
    
    # Deduplication
    window_spec = Window.partitionBy("customer_id").orderBy(col("bronze_ingestion_time").desc())
    
    silver_customers = bronze_customers \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num") \
        .withColumn("silver_processed_time", current_timestamp())
    
    output_path = "s3a://silver-layer/customers"
    
    silver_customers.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = silver_customers.count()
    logger.info(f"[SILVER] Customers cleaned: {count} records -> {output_path}")
    
    return silver_customers


def silver_clean_payment_method(spark: SparkSession):
    """Clean payment_method for Silver layer"""
    logger.info("=== [SILVER] Cleaning payment_method ===")
    
    bronze_payment = spark.read.parquet("s3a://bronze-layer/payment_method")
    
    silver_payment = bronze_payment \
        .withColumn("silver_processed_time", current_timestamp()) \
        .dropDuplicates(["payment_method_id"])
    
    output_path = "s3a://silver-layer/payment_method"
    
    silver_payment.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = silver_payment.count()
    logger.info(f"[SILVER] Payment methods cleaned: {count} records -> {output_path}")
    
    return silver_payment


# =====================================================================
# GOLD LAYER - Star Schema (Dimensional Model)
# =====================================================================

def gold_dim_customer(spark: SparkSession):
    """Create dim_customer dimension table"""
    logger.info("=== [GOLD] Building dim_customer ===")
    
    silver_customers = spark.read.parquet("s3a://silver-layer/customers")
    
    dim_customer = silver_customers.select(
        col("customer_id"),
        col("customer_name").alias("name"),
        col("email"),
        col("phone"),
        col("address"),
        coalesce(col("city"), lit("Unknown")).alias("city"),
        coalesce(col("state"), lit("Unknown")).alias("state"),
        coalesce(col("country"), lit("Unknown")).alias("country"),
        lit("Standard").alias("customer_segment"),  # Default segment
        current_timestamp().alias("dim_created_at"),
        current_timestamp().alias("dim_updated_at")
    )
    
    output_path = "s3a://gold-layer/dimensions/dim_customer"
    
    dim_customer.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = dim_customer.count()
    logger.info(f"[GOLD] dim_customer created: {count} records -> {output_path}")
    
    return dim_customer


def gold_dim_payment_method(spark: SparkSession):
    """Create dim_payment_method dimension table"""
    logger.info("=== [GOLD] Building dim_payment_method ===")
    
    silver_payment = spark.read.parquet("s3a://silver-layer/payment_method")
    
    dim_payment = silver_payment.select(
        col("payment_method_id"),
        col("payment_method_name").alias("method_name"),
        when(col("payment_method_name").like("%Credit%"), "Credit Card")
            .when(col("payment_method_name").like("%Debit%"), "Debit Card")
            .when(col("payment_method_name").like("%Cash%"), "Cash")
            .otherwise("Other").alias("payment_type"),
        lit(1).alias("is_active"),
        current_timestamp().alias("dim_created_at"),
        current_timestamp().alias("dim_updated_at")
    )
    
    output_path = "s3a://gold-layer/dimensions/dim_payment_method"
    
    dim_payment.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = dim_payment.count()
    logger.info(f"[GOLD] dim_payment_method created: {count} records -> {output_path}")
    
    return dim_payment


def gold_dim_product(spark: SparkSession):
    """Create dim_product dimension table (from order_details product_ids)"""
    logger.info("=== [GOLD] Building dim_product ===")
    
    silver_order_details = spark.read.parquet("s3a://silver-layer/order_details")
    
    # Extract unique products
    dim_product = silver_order_details.select("product_id").distinct() \
        .withColumn("product_name", concat(lit("Product_"), col("product_id"))) \
        .withColumn("category", lit("General")) \
        .withColumn("subcategory", lit("Standard")) \
        .withColumn("brand", lit("Unknown")) \
        .withColumn("price", lit(0.0)) \
        .withColumn("product_status", lit("Active")) \
        .withColumn("dim_created_at", current_timestamp()) \
        .withColumn("dim_updated_at", current_timestamp())
    
    output_path = "s3a://gold-layer/dimensions/dim_product"
    
    dim_product.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = dim_product.count()
    logger.info(f"[GOLD] dim_product created: {count} records -> {output_path}")
    
    return dim_product


def gold_dim_date(spark: SparkSession):
    """Create dim_date dimension table with date attributes"""
    logger.info("=== [GOLD] Building dim_date ===")
    
    # Generate date range (3 years: 2024-2027)
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2027, 12, 31)
    
    dates = []
    current = start_date
    while current <= end_date:
        dates.append({
            "date_id": int(current.strftime("%Y%m%d")),
            "date": current.date(),
            "year": current.year,
            "quarter": (current.month - 1) // 3 + 1,
            "month": current.month,
            "month_name": current.strftime("%B"),
            "day": current.day,
            "day_of_week": current.weekday() + 1,  # 1=Monday, 7=Sunday
            "day_name": current.strftime("%A"),
            "is_weekend": 1 if current.weekday() >= 5 else 0,
            "is_holiday": 0,  # Can be updated with holiday logic
            "week_of_year": current.isocalendar()[1]
        })
        current += timedelta(days=1)
    
    dim_date_schema = StructType([
        StructField("date_id", IntegerType(), False),
        StructField("date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("month_name", StringType(), False),
        StructField("day", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_name", StringType(), False),
        StructField("is_weekend", IntegerType(), False),
        StructField("is_holiday", IntegerType(), False),
        StructField("week_of_year", IntegerType(), False)
    ])
    
    dim_date = spark.createDataFrame(dates, schema=dim_date_schema)
    
    output_path = "s3a://gold-layer/dimensions/dim_date"
    
    dim_date.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = dim_date.count()
    logger.info(f"[GOLD] dim_date created: {count} records -> {output_path}")
    
    return dim_date


def gold_fact_orders(spark: SparkSession):
    """Create fact_orders fact table (Star Schema center)"""
    logger.info("=== [GOLD] Building fact_orders ===")
    
    # Read silver data
    silver_orders = spark.read.parquet("s3a://silver-layer/orders")
    silver_order_details = spark.read.parquet("s3a://silver-layer/order_details")
    
    # Aggregate order details (count products per order)
    order_aggregates = silver_order_details.groupBy("order_id").agg(
        count("product_id").alias("product_count"),
        sum("quantity").alias("total_quantity"),
        collect_list("product_id").alias("product_ids")
    )
    
    # Join with orders
    fact_orders = silver_orders.join(order_aggregates, "order_id", "left") \
        .withColumn("order_date_id", 
                   date_format(col("order_created_at"), "yyyyMMdd").cast(IntegerType())) \
        .withColumn("order_status", lit("completed")) \
        .withColumn("total_amount", lit(0.0))  # To be calculated if price data available
    
    # Select final fact table columns
    fact_orders_final = fact_orders.select(
        col("order_id"),
        col("customer_id"),
        col("payment_method_id"),
        col("order_date_id"),
        coalesce(col("product_count"), lit(0)).alias("product_count"),
        coalesce(col("num_products"), lit(0)).alias("expected_products"),
        coalesce(col("total_quantity"), lit(0)).alias("quantity_ordered"),
        col("total_amount"),
        col("order_status"),
        col("order_created_at").alias("created_timestamp"),
        current_timestamp().alias("fact_created_at")
    )
    
    output_path = "s3a://gold-layer/facts/fact_orders"
    
    fact_orders_final.write \
        .mode("overwrite") \
        .partitionBy("order_date_id") \
        .parquet(output_path)
    
    count = fact_orders_final.count()
    logger.info(f"[GOLD] fact_orders created: {count} records -> {output_path}")
    
    return fact_orders_final


def gold_fact_order_products(spark: SparkSession):
    """Create fact_order_products bridge table (many-to-many relationship)"""
    logger.info("=== [GOLD] Building fact_order_products ===")
    
    silver_order_details = spark.read.parquet("s3a://silver-layer/order_details")
    
    fact_order_products = silver_order_details.select(
        col("order_id"),
        col("product_id"),
        coalesce(col("quantity"), lit(1)).alias("quantity"),
        lit(0.0).alias("unit_price"),
        lit(0.0).alias("line_total"),
        current_timestamp().alias("fact_created_at")
    )
    
    output_path = "s3a://gold-layer/facts/fact_order_products"
    
    fact_order_products.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = fact_order_products.count()
    logger.info(f"[GOLD] fact_order_products created: {count} records -> {output_path}")
    
    return fact_order_products


# =====================================================================
# MAIN ORCHESTRATION
# =====================================================================

def run_bronze_layer(spark: SparkSession):
    """Execute all Bronze layer ingestion"""
    logger.info("\n" + "="*70)
    logger.info("STARTING BRONZE LAYER INGESTION")
    logger.info("="*70)
    
    bronze_ingest_orders(spark)
    bronze_ingest_order_details(spark)
    bronze_ingest_customers(spark)
    bronze_ingest_payment_method(spark)
    
    logger.info("[BRONZE] Layer completed successfully\n")


def run_silver_layer(spark: SparkSession):
    """Execute all Silver layer transformations"""
    logger.info("\n" + "="*70)
    logger.info("STARTING SILVER LAYER TRANSFORMATION")
    logger.info("="*70)
    
    silver_clean_orders(spark)
    silver_clean_order_details(spark)
    silver_clean_customers(spark)
    silver_clean_payment_method(spark)
    
    logger.info("[SILVER] Layer completed successfully\n")


def run_gold_layer(spark: SparkSession):
    """Execute all Gold layer star schema creation"""
    logger.info("\n" + "="*70)
    logger.info("STARTING GOLD LAYER - STAR SCHEMA")
    logger.info("="*70)
    
    # Create dimensions first
    gold_dim_customer(spark)
    gold_dim_payment_method(spark)
    gold_dim_product(spark)
    gold_dim_date(spark)
    
    # Create facts
    gold_fact_orders(spark)
    gold_fact_order_products(spark)
    
    logger.info("[GOLD] Layer completed successfully\n")


def main():
    """Main ETL pipeline execution"""
    logger.info("\n" + "="*70)
    logger.info("BRONZE-SILVER-GOLD STAR SCHEMA ETL PIPELINE")
    logger.info(f"Started at: {datetime.now()}")
    logger.info("="*70 + "\n")
    
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Execute layers in sequence
        run_bronze_layer(spark)
        run_silver_layer(spark)
        run_gold_layer(spark)
        
        logger.info("\n" + "="*70)
        logger.info("✅ ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Finished at: {datetime.now()}")
        logger.info("="*70)
        
        # Print summary
        logger.info("\n📊 STAR SCHEMA SUMMARY:")
        logger.info("Dimensions:")
        logger.info("  - dim_customer")
        logger.info("  - dim_payment_method")
        logger.info("  - dim_product")
        logger.info("  - dim_date")
        logger.info("\nFacts:")
        logger.info("  - fact_orders (main fact table)")
        logger.info("  - fact_order_products (bridge table)")
        
    except Exception as e:
        logger.error(f"❌ ETL Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()
