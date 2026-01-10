

import logging
import multiprocessing
from pathlib import Path

import redis
from kafka_handler import KafkaHandler

# Logging Configuration
BASE_DIR = Path(__file__).resolve().parent.parent.parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "logs" / "real-time.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Redis Configuration
redis_dynamic = redis.Redis(host="localhost", port=6379, db=1, decode_responses=True)


def check_and_trigger(order_id, producer):
    """Check if order is complete and trigger processing with atomic lock."""
    # Use Redis SET NX to implement atomic mutex - ensures only one trigger per order
    lock_key = f"order_trigger_lock:{order_id}"
    if not redis_dynamic.set(lock_key, "1", nx=True, ex=10):  # 10 seconds lock
        logger.debug(f"[TRIGGER_LOCKED] {order_id} already being triggered by another worker")
        return
    
    try:
        # Double-check: verify order hasn't been triggered already
        if redis_dynamic.get(f"order_status:{order_id}") == "checking":
            logger.debug(f"[ALREADY_TRIGGERED] {order_id} already marked as checking")
            return
        
        order_info = redis_dynamic.hgetall(f"order_info:{order_id}")
        if not order_info:
            logger.debug(f"[ORDER_INFO_MISSING] {order_id} waiting for order_info in Redis")
            return
        
        num_products = int(order_info.get("num_products", 0))
        current_products = redis_dynamic.smembers(f"ordered_products:{order_id}")
        
        if len(current_products) == num_products:
            logger.info(f"[ORDER_COMPLETE] {order_id} with {num_products} products")
            producer.send("order_final", {
                "order_id": order_id,
                "customer_id": order_info["customer_id"],
                "payment_method_id": order_info["payment_method_id"],
                "product_ids": list(current_products),
            })
        
            redis_dynamic.set(f"order_status:{order_id}", "checking")
            redis_dynamic.expire(f"order_status:{order_id}", 300)  # 5 minutes TTL
            
            redis_dynamic.delete(f"order_info:{order_id}")
            redis_dynamic.delete(f"ordered_products:{order_id}")
        else:
            logger.debug(f"[PRODUCTS_INCOMPLETE] {order_id} (current={len(current_products)}, expected={num_products})")
    finally:
        # Release lock
        redis_dynamic.delete(lock_key)


def process_message(message, producer):
    """Process order detail message from Kafka."""
    try:
        order_detail_payload = message.value.get("payload", {})
        if "after" not in order_detail_payload:
            logger.warning(f"Invalid message structure: {message.value}")
            return
            
        order_detail_data = order_detail_payload["after"]
        order_id = order_detail_data["order_id"]
        product_id = order_detail_data["product_id"]

        if redis_dynamic.get(f"order_status:{order_id}") == "checking":
            return
        
        # Add product_id to the Redis set for this order (to track all ordered products)
        redis_dynamic.sadd(f"ordered_products:{order_id}", product_id)
        redis_dynamic.expire(f"ordered_products:{order_id}", 120)  # 2 minutes TTL
        
        logger.info(f"[PRODUCT_ADDED] Order {order_id} - Product {product_id}")
        
        check_and_trigger(order_id, producer)
        
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)


def ordered_products_worker(worker_id: int):
    """Worker process to consume order detail messages."""
    logger.info(f"Starting ordered_products_worker {worker_id}")
    
    # Use external Kafka ports for localhost connection
    bootstrap_servers = ["localhost:9093", "localhost:9095"]
    kafka_client = KafkaHandler(bootstrap_servers)
    producer = kafka_client.create_producer()
    consumer = kafka_client.create_consumer(
        topic="mysql.project_db.order_details",
        group_id="order_products_tracker"
    )

    try:
        logger.info(f"Worker {worker_id} consuming from mysql.project_db.order_details")
        while True:
            message_pack = consumer.poll(timeout_ms=1000)
            for _, messages in message_pack.items():
                for message in messages:
                    process_message(message, producer)
    except KeyboardInterrupt:
        logger.info(f"Worker {worker_id} received shutdown signal")
    except Exception as e:
        logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        logger.info(f"Worker {worker_id} closed")


def main():
    """Start multiple worker processes."""
    num_workers = 3
    processes = []

    logger.info(f"Starting {num_workers} order detail tracking workers...")
    
    for i in range(num_workers):
        p = multiprocessing.Process(target=ordered_products_worker, args=(i,))
        p.start()
        processes.append(p)
        logger.info(f"Started worker {i} (PID: {p.pid})")

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("Main process received shutdown signal")
        for p in processes:
            p.terminate()
            p.join()
        logger.info("All workers terminated")

    
if __name__ == "__main__":
    main()