"""
Bronze Layer - Raw Data Ingestion from MySQL to MinIO
Supports incremental load with year/month/day partitioning
"""

import os
import sys
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Setup paths
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

# Setup logging
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "bronze_layer.log"

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
    """Create Spark session for Docker environment"""
    return SparkSession.builder \
        .appName("Bronze Layer - Data Ingestion") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "4") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
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


def check_bronze_data_exists(spark: SparkSession, path: str) -> bool:
    """Check if Bronze layer already has data"""
    try:
        spark.read.parquet(path)
        logger.info(f"[CHECK] Data exists at {path}")
        return True
    except Exception as e:
        logger.info(f"[CHECK] No data at {path}")
        return False


def bronze_ingest_orders(spark: SparkSession):
    """Ingest orders table to Bronze layer with incremental load support"""
    logger.info("=== [BRONZE] Ingesting orders ===")
    
    output_path = "s3a://bronze/orders"
    
    # Read from MySQL
    orders_df = read_mysql_table(spark, "orders")
    
    # Check if Bronze data exists
    if check_bronze_data_exists(spark, output_path):
        logger.info("[BRONZE][orders] Existing data found -> Incremental load")
        
        # Read existing Bronze data
        existing_df = spark.read.parquet(output_path)
        
        # Find latest timestamp
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
            write_mode = "append"
        else:
            logger.warning("[BRONZE][orders] No timestamp column found, doing full load")
            orders_to_process = orders_df
            write_mode = "overwrite"
    else:
        logger.info("[BRONZE][orders] No existing data -> Full load")
        orders_to_process = orders_df
        write_mode = "overwrite"
    
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
    
    # Write to Bronze layer (write_mode already determined above)
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
    
    output_path = "s3a://bronze/order_details"
    
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
            write_mode = "append"
        else:
            logger.warning("[BRONZE][order_details] No timestamp column, doing full load")
            details_to_process = order_details_df
            write_mode = "overwrite"
    else:
        logger.info("[BRONZE][order_details] No existing data -> Full load")
        details_to_process = order_details_df
        write_mode = "overwrite"
    
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
    
    # Write to Bronze layer (write_mode already determined above)
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
    
    output_path = "s3a://bronze/customers"
    
    # Read from MySQL
    customers_df = read_mysql_table(spark, "customers")
    
    # Check if Bronze data exists
    write_mode = "overwrite"  # Default for full load
    
    if check_bronze_data_exists(spark, output_path):
        logger.info("[BRONZE][customers] Existing data found -> Incremental load")
        
        existing_df = spark.read.parquet(output_path)
        
        timestamp_col = None
        for col_name in ["timestamp", "created_at", "updated_at"]:
            if col_name in customers_df.columns:
                timestamp_col = col_name
                break
        
        if timestamp_col:
            max_timestamp = existing_df.select(max(col(timestamp_col))).first()[0]
            logger.info(f"[BRONZE][customers] Latest timestamp: {max_timestamp}")
            
            # Filter only new records (INCREMENTAL)
            new_customers = customers_df.filter(col(timestamp_col) > max_timestamp)
            new_count = new_customers.count()
            
            if new_count == 0:
                logger.info("[BRONZE][customers] No new records to ingest")
                return existing_df
            
            logger.info(f"[BRONZE][customers] Found {new_count} new records")
            customers_to_process = new_customers
            write_mode = "append"  # Incremental uses append
        else:
            logger.warning("[BRONZE][customers] No timestamp column, doing full load")
            customers_to_process = customers_df
            write_mode = "overwrite"  # Full reload
    else:
        logger.info("[BRONZE][customers] No existing data -> FULL LOAD (all records from MySQL)")
        customers_to_process = customers_df
        write_mode = "overwrite"  # First time = full load
    
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
    
    # Write to Bronze layer (write_mode already determined above)
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
    
    output_path = "s3a://bronze/payment_method"
    
    # Read from MySQL
    payment_df = read_mysql_table(spark, "payment_method")
    
    # Check if Bronze data exists
    write_mode = "overwrite"  # Default for full load
    
    if check_bronze_data_exists(spark, output_path):
        logger.info("[BRONZE][payment_method] Existing data found -> Incremental load")
        
        existing_df = spark.read.parquet(output_path)
        
        timestamp_col = None
        for col_name in ["timestamp", "created_at", "updated_at"]:
            if col_name in payment_df.columns:
                timestamp_col = col_name
                break
        
        if timestamp_col:
            max_timestamp = existing_df.select(max(col(timestamp_col))).first()[0]
            logger.info(f"[BRONZE][payment_method] Latest timestamp: {max_timestamp}")
            
            # Filter only new records (INCREMENTAL)
            new_payment = payment_df.filter(col(timestamp_col) > max_timestamp)
            new_count = new_payment.count()
            
            if new_count == 0:
                logger.info("[BRONZE][payment_method] No new records to ingest")
                return existing_df
            
            logger.info(f"[BRONZE][payment_method] Found {new_count} new records")
            payment_to_process = new_payment
            write_mode = "append"  # Incremental uses append
        else:
            logger.warning("[BRONZE][payment_method] No timestamp column, doing full load")
            payment_to_process = payment_df
            write_mode = "overwrite"  # Full reload
    else:
        logger.info("[BRONZE][payment_method] No existing data -> FULL LOAD (all records from MySQL)")
        payment_to_process = payment_df
        write_mode = "overwrite"  # First time = full load
    
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
    
    # Write to Bronze layer (write_mode already determined above)
    bronze_payment.write \
        .mode(write_mode) \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)
    
    count = bronze_payment.count()
    logger.info(f"[BRONZE] Payment methods ingested: {count} records -> {output_path}")
    
    return bronze_payment


def main():
    """Main Bronze layer execution"""
    logger.info("\n" + "="*70)
    logger.info("BRONZE LAYER - RAW DATA INGESTION")
    logger.info("="*70)
    
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Ingest all tables
        bronze_ingest_orders(spark)
        bronze_ingest_order_details(spark)
        bronze_ingest_customers(spark)
        bronze_ingest_payment_method(spark)
        
        logger.info("\n" + "="*70)
        logger.info("✅ BRONZE LAYER COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        
    except Exception as e:
        logger.error(f"❌ Bronze layer failed: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()
