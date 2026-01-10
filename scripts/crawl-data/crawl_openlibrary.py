import os
import aiohttp
import asyncio
import logging
import sys
import random
from pathlib import Path

# Define BASE_DIR - root project directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Fix import path
sys.path.insert(0, os.path.dirname(__file__))

from checkpoint import load_checkpoint, save_checkpoint
from db_helper import create_books_table, bulk_insert_books, get_books_count

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Open Library API configuration
BASE_URL = "https://openlibrary.org/search.json?q=fiction&limit={}&offset={}"
LIMIT = 100  
TOTAL_RECORDS = 100000 
CONCURRENCY = 4  # Tăng nhẹ concurrency
MAX_BATCH_SIZE = 500
REQUEST_DELAY = 1.5  # Giảm nhẹ delay xuống 1.5 giây  

SEMAPHORE = asyncio.Semaphore(CONCURRENCY)
last_request_time = 0

def extract_list(data, key, default=""):
    """Extract first element from list or return default."""
    if isinstance(data, list) and len(data) > 0:
        return str(data[0])
    return default

def extract_list_str(data, key, default=""):
    """Extract and join list elements into comma-separated string."""
    if isinstance(data, list) and len(data) > 0:
        return ", ".join([str(x) for x in data[:5]])  # Limit to first 5 items
    return default

async def fetch_books(session, offset):
    """Fetch books from Open Library API at given offset."""
    global last_request_time
    
    # Rate limiting: ensure at least REQUEST_DELAY seconds between requests
    now = asyncio.get_event_loop().time()
    sleep_time = max(0, REQUEST_DELAY - (now - last_request_time))
    if sleep_time > 0:
        await asyncio.sleep(sleep_time)
    last_request_time = asyncio.get_event_loop().time()
    
    url = BASE_URL.format(LIMIT, offset)
    timeout = aiohttp.ClientTimeout(total=180)
    
    for attempt in range(8):
        try:
            async with session.get(url, timeout=timeout) as res:
                if res.status == 200:
                    data = await res.json()
                    logging.info(f"Offset {offset} fetched successfully")
                    return data.get("docs", [])
                elif res.status == 429:  # Rate limit
                    logging.warning(f"Offset {offset} rate limited (429), attempt {attempt+1}/8")
                    if attempt < 7:
                        wait_time = 10 * (2 ** attempt)
                        logging.info(f"Rate limited - waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                else:
                    logging.warning(f"Offset {offset} HTTP {res.status}, attempt {attempt+1}/8")
                    if attempt < 7:
                        await asyncio.sleep(5 * (2 ** attempt))
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            logging.warning(f"Offset {offset} {type(e).__name__}, attempt {attempt+1}/8")
            if attempt < 7:
                wait_time = 15 * (2 ** attempt)
                logging.info(f"Waiting {wait_time}s before retry")
                await asyncio.sleep(wait_time)
        except Exception as e:
            logging.warning(f"Offset {offset} {type(e).__name__}: {str(e)}, attempt {attempt+1}/8")
            if attempt < 7:
                await asyncio.sleep(5 * (2 ** attempt))
    
    logging.error(f"Failed to fetch offset {offset} after 8 attempts")
    return []

async def process_offset(session, offset, books_buffer):
    """Process books at given offset and add to buffer."""
    async with SEMAPHORE:
        docs = await fetch_books(session, offset)
        processed_books = []
        
        for doc in docs:
            # Extract book information
            work_key = doc.get("key", "").replace("/works/", "")
            if not work_key:
                continue
            
            # Generate fake price between 5 and 50 USD
            unit_price = round(random.uniform(5.0, 50.0), 2)
            
            book_data = (
                work_key,
                doc.get("title", "")[:500] if doc.get("title") else "",
                extract_list_str(doc.get("author_name"), "author_name")[:500],
                extract_list_str(doc.get("author_key"), "author_key")[:500],
                doc.get("first_publish_year") if doc.get("first_publish_year") else None,
                extract_list_str(doc.get("isbn"), "isbn")[:500],
                extract_list_str(doc.get("language"), "language")[:200],
                str(doc.get("cover_i", "")) if doc.get("cover_i") else "",
                unit_price
            )
            
            processed_books.append(book_data)
        
        books_buffer.extend(processed_books)
        return len(processed_books)

async def crawl_async():
    """Main crawl function."""
    # Create table if not exists
    create_books_table()
    
    start_offset = load_checkpoint()
    logging.info(f"Resuming from offset {start_offset}")
    logging.info(f"Current books in database: {get_books_count()}")
    
    books_buffer = []
    current_offset = start_offset
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        
        for offset in range(start_offset, TOTAL_RECORDS, LIMIT):
            tasks.append(process_offset(session, offset, books_buffer))
            current_offset = offset
            
            # Process tasks in batches
            if len(tasks) >= CONCURRENCY:
                results = await asyncio.gather(*tasks)
                books_fetched = sum(results)
                tasks = []
                
                logging.info(f"Processed offset {offset}, buffer size: {len(books_buffer)}")
                
                # Save checkpoint after successful batch
                save_checkpoint(offset)
                
                # Insert to database when buffer reaches threshold
                if len(books_buffer) >= MAX_BATCH_SIZE:
                    try:
                        rows_inserted = bulk_insert_books(books_buffer)
                        logging.info(f"Inserted {rows_inserted} books to MySQL. Total in DB: {get_books_count()}")
                        books_buffer = []  # Clear buffer after successful insert
                    except Exception as e:
                        logging.error(f"Failed to insert books: {str(e)}")
                        # Don't clear buffer on failure, will retry in next iteration
        
        # Process remaining tasks
        if tasks:
            results = await asyncio.gather(*tasks)
            books_fetched = sum(results)
            logging.info(f"Processed final batch at offset {current_offset}, buffer size: {len(books_buffer)}")
            save_checkpoint(current_offset)
        
        # Insert remaining books in buffer
        if books_buffer:
            try:
                rows_inserted = bulk_insert_books(books_buffer)
                logging.info(f"Inserted final {rows_inserted} books to MySQL. Total in DB: {get_books_count()}")
            except Exception as e:
                logging.error(f"Failed to insert final batch: {str(e)}")
    
    logging.info(f"Crawl completed. Total books in database: {get_books_count()}")

def run_crawl():
    """Entry point for crawl."""
    asyncio.run(crawl_async())

if __name__ == "__main__":
    run_crawl()
