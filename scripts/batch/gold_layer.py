"""
Gold Layer - Star Schema (Dimensional Model)
fact_orders + dimensions (customer, payment_method, product, date)
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import count as spark_count, sum as spark_sum
from pyspark.sql.window import Window

# Setup paths
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

# Setup logging
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "gold_layer.log"

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
        .appName("Gold Layer - Star Schema") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "4") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir", "s3a://gold/warehouse") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


# =====================================================================
# DIMENSIONS
# =====================================================================

def gold_dim_customer(spark: SparkSession):
    """Create dim_customer dimension table"""
    logger.info("=== [GOLD] Building dim_customer ===")
    
    silver_customers = spark.read.parquet("s3a://silver/customers")
    
    # Use actual columns: id, name, phone_number, tier
    dim_customer = silver_customers.select(
        col("id").alias("customer_id"),
        col("name").alias("customer_name"),
        col("phone_number").alias("phone"),
        col("tier").alias("customer_segment"),
        col("updated_at"),
        current_timestamp().alias("dim_created_at"),
        current_timestamp().alias("dim_updated_at")
    )
    
    output_path = "s3a://gold/dimensions/dim_customer"
    
    dim_customer.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = dim_customer.count()
    logger.info(f"[GOLD] dim_customer created: {count} records -> {output_path}")
    
    return dim_customer


def gold_dim_payment_method(spark: SparkSession):
    """Create dim_payment_method dimension table"""
    logger.info("=== [GOLD] Building dim_payment_method ===")
    
    silver_payment = spark.read.parquet("s3a://silver/payment_method")
    
    # Use actual columns: id, method_name, bank
    dim_payment = silver_payment.select(
        col("id").alias("payment_method_id"),
        col("method_name"),
        col("bank"),
        when(col("method_name").like("%Credit%"), "Credit Card")
            .when(col("method_name").like("%Debit%"), "Debit Card")
            .when(col("method_name").like("%Cash%"), "Cash")
            .otherwise("Other").alias("payment_type"),
        lit(1).alias("is_active"),
        current_timestamp().alias("dim_created_at"),
        current_timestamp().alias("dim_updated_at")
    )
    
    output_path = "s3a://gold/dimensions/dim_payment_method"
    
    dim_payment.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = dim_payment.count()
    logger.info(f"[GOLD] dim_payment_method created: {count} records -> {output_path}")
    
    return dim_payment


def gold_dim_product(spark: SparkSession):
    """Create dim_product dimension table"""
    logger.info("=== [GOLD] Building dim_product ===")
    
    silver_order_details = spark.read.parquet("s3a://silver/order_details")
    
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
    
    output_path = "s3a://gold/dimensions/dim_product"
    
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
            "day_of_week": current.weekday() + 1,
            "day_name": current.strftime("%A"),
            "is_weekend": 1 if current.weekday() >= 5 else 0,
            "is_holiday": 0,
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
    
    output_path = "s3a://gold/dimensions/dim_date"
    
    dim_date.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = dim_date.count()
    logger.info(f"[GOLD] dim_date created: {count} records -> {output_path}")
    
    return dim_date


# =====================================================================
# FACTS
# =====================================================================

def gold_fact_orders(spark: SparkSession):
    """Create fact_orders fact table (Star Schema center)"""
    logger.info("=== [GOLD] Building fact_orders ===")
    
    # Read silver data
    silver_orders = spark.read.parquet("s3a://silver/orders")
    silver_order_details = spark.read.parquet("s3a://silver/order_details")
    
    # Aggregate order details (count products per order)
    order_aggregates = silver_order_details.groupBy("order_id").agg(
        spark_count("product_id").alias("product_count"),
        spark_sum("quantity").alias("total_quantity"),
        collect_list("product_id").alias("product_ids")
    )
    
    # Join with orders
    fact_orders = silver_orders.join(order_aggregates, "order_id", "left")
    
    # Create order_date_id from order_created_at or timestamp
    if "order_created_at" in fact_orders.columns:
        fact_orders = fact_orders.withColumn("order_date_id", 
                                             date_format(col("order_created_at"), "yyyyMMdd").cast(IntegerType()))
    elif "timestamp" in fact_orders.columns:
        fact_orders = fact_orders.withColumn("order_date_id", 
                                             date_format(col("timestamp"), "yyyyMMdd").cast(IntegerType()))
    else:
        fact_orders = fact_orders.withColumn("order_date_id", 
                                             date_format(current_timestamp(), "yyyyMMdd").cast(IntegerType()))
    
    fact_orders = fact_orders \
        .withColumn("order_status", lit("completed")) \
        .withColumn("total_amount", lit(0.0))
    
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
        col("timestamp").alias("created_timestamp"),
        current_timestamp().alias("fact_created_at")
    )
    
    output_path = "s3a://gold/facts/fact_orders"
    
    fact_orders_final.write \
        .mode("overwrite") \
        .partitionBy("order_date_id") \
        .parquet(output_path)
    
    count = fact_orders_final.count()
    logger.info(f"[GOLD] fact_orders created: {count} records -> {output_path}")
    
    return fact_orders_final


def gold_fact_order_products(spark: SparkSession):
    """Create fact_order_products bridge table"""
    logger.info("=== [GOLD] Building fact_order_products ===")
    
    silver_order_details = spark.read.parquet("s3a://silver/order_details")
    
    fact_order_products = silver_order_details.select(
        col("order_id"),
        col("product_id"),
        coalesce(col("quantity"), lit(1)).alias("quantity"),
        lit(0.0).alias("unit_price"),
        lit(0.0).alias("line_total"),
        current_timestamp().alias("fact_created_at")
    )
    
    output_path = "s3a://gold/facts/fact_order_products"
    
    fact_order_products.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    count = fact_order_products.count()
    logger.info(f"[GOLD] fact_order_products created: {count} records -> {output_path}")
    
    return fact_order_products


# =====================================================================
# MAIN
# =====================================================================

def main():
    """Main Gold layer execution"""
    logger.info("\n" + "="*70)
    logger.info("GOLD LAYER - STAR SCHEMA")
    logger.info("="*70)
    
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Create dimensions first
        logger.info("\n[GOLD] Creating dimensions...")
        gold_dim_customer(spark)
        gold_dim_payment_method(spark)
        gold_dim_product(spark)
        gold_dim_date(spark)
        
        # Create facts
        logger.info("\n[GOLD] Creating facts...")
        gold_fact_orders(spark)
        gold_fact_order_products(spark)
        
        logger.info("\n" + "="*70)
        logger.info("✅ GOLD LAYER COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        
        logger.info("\n📊 STAR SCHEMA SUMMARY:")
        logger.info("Dimensions:")
        logger.info("  - dim_customer")
        logger.info("  - dim_payment_method")
        logger.info("  - dim_product")
        logger.info("  - dim_date")
        logger.info("\nFacts:")
        logger.info("  - fact_orders")
        logger.info("  - fact_order_products")
        
    except Exception as e:
        logger.error(f"❌ Gold layer failed: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()
