"""
Silver Layer - Data Cleaning and Transformation
Deduplication, quality checks, standardization
"""

import os
import sys
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Setup paths
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

# Setup logging
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "silver_layer.log"

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
        .appName("Silver Layer - Data Cleaning") \
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


def silver_clean_orders(spark: SparkSession):
    """Clean and deduplicate orders for Silver layer"""
    logger.info("=== [SILVER] Cleaning orders ===")
    
    bronze_orders = spark.read.parquet("s3a://bronze/orders")
    
    # Deduplication based on order id (keep most recent)
    window_spec = Window.partitionBy("id").orderBy(col("bronze_ingestion_time").desc())
    
    silver_orders = bronze_orders \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num") \
        .withColumn("silver_processed_time", current_timestamp()) \
        .withColumnRenamed("id", "order_id")
    
    # Rename created_at if exists
    if "created_at" in silver_orders.columns:
        silver_orders = silver_orders.withColumnRenamed("created_at", "order_created_at")
    
    output_path = "s3a://silver/orders"
    
    silver_orders.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = silver_orders.count()
    logger.info(f"[SILVER] Orders cleaned: {count} records -> {output_path}")
    
    return silver_orders


def silver_clean_order_details(spark: SparkSession):
    """Clean order_details for Silver layer"""
    logger.info("=== [SILVER] Cleaning order_details ===")
    
    bronze_order_details = spark.read.parquet("s3a://bronze/order_details")
    
    # Deduplication
    window_spec = Window.partitionBy("order_id", "product_id").orderBy(col("bronze_ingestion_time").desc())
    
    silver_order_details = bronze_order_details \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num") \
        .withColumn("silver_processed_time", current_timestamp()) \
        .withColumn("quantity", lit(1))  # Default quantity if not exists
    
    output_path = "s3a://silver/order_details"
    
    silver_order_details.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = silver_order_details.count()
    logger.info(f"[SILVER] Order details cleaned: {count} records -> {output_path}")
    
    return silver_order_details


def silver_clean_customers(spark: SparkSession):
    """Clean customers for Silver layer"""
    logger.info("=== [SILVER] Cleaning customers ===")
    
    bronze_customers = spark.read.parquet("s3a://bronze/customers")
    
    # Deduplication by id (not customer_id)
    window_spec = Window.partitionBy("id").orderBy(col("bronze_ingestion_time").desc())
    
    silver_customers = bronze_customers \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num") \
        .withColumn("silver_processed_time", current_timestamp())
    
    output_path = "s3a://silver/customers"
    
    silver_customers.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = silver_customers.count()
    logger.info(f"[SILVER] Customers cleaned: {count} records -> {output_path}")
    
    return silver_customers


def silver_clean_payment_method(spark: SparkSession):
    """Clean payment_method for Silver layer"""
    logger.info("=== [SILVER] Cleaning payment_method ===")
    
    bronze_payment = spark.read.parquet("s3a://bronze/payment_method")
    
    silver_payment = bronze_payment \
        .withColumn("silver_processed_time", current_timestamp()) \
        .dropDuplicates(["id"])
    
    output_path = "s3a://silver/payment_method"
    
    silver_payment.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = silver_payment.count()
    logger.info(f"[SILVER] Payment methods cleaned: {count} records -> {output_path}")
    
    return silver_payment


def main():
    """Main Silver layer execution"""
    logger.info("\n" + "="*70)
    logger.info("SILVER LAYER - DATA CLEANING & TRANSFORMATION")
    logger.info("="*70)
    
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Clean all tables
        silver_clean_orders(spark)
        silver_clean_order_details(spark)
        silver_clean_customers(spark)
        silver_clean_payment_method(spark)
        
        logger.info("\n" + "="*70)
        logger.info("✅ SILVER LAYER COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        
    except Exception as e:
        logger.error(f"❌ Silver layer failed: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()
