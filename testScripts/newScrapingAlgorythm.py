import os
from dotenv import load_dotenv
from urllib.parse import urlparse
import psycopg2
import pyautogui
import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import logging
import undetected_chromedriver as uc
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor, as_completed
from screeninfo import get_monitors
import requests
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Add at the top with other globals
connection_pool = None

def initialize_connection_pool():
    load_dotenv()
    database_url = os.getenv('STAGING_DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL not found in .env file")
        return None

    parsed_url = urlparse(database_url)
    dbname = parsed_url.path[1:]  
    user = parsed_url.username
    password = parsed_url.password
    host = parsed_url.hostname
    port = parsed_url.port or 5432 

    try:
        # Create a pool with minimum 1 connection, maximum 10 connections
        pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            sslmode='require'
        )
        logger.info("Connection pool created successfully!")
        return pool
    except psycopg2.Error as e:
        logger.error(f"Error creating connection pool: {e}")
        return None

# def get_test_urls(pool):
#     conn = None
#     try:
#         conn = pool.getconn()  # Get connection from pool
#         cursor = conn.cursor()
#         cursor.execute("""
#             SELECT DISTINCT tcgplayer_url 
#             FROM prize 
#             WHERE tcgplayer_url IS NOT NULL 
#             AND is_deleted = false
#             AND is_manually_priced = false
#             LIMIT 6
#         """)
#         urls = [row[0] for row in cursor.fetchall()]
#         logger.info(f"Retrieved {len(urls)} unique URLs")
#         return urls
#     finally:
#         if conn:
#             pool.putconn(conn)  # Return connection to pool

def get_test_urls(pool):
    conn = None
    try:
        conn = pool.getconn()
        urls = ["https://www.tcgplayer.com/product/45123?ListingType=standard&page=1&Condition=Lightly+Played|Near+Mint&Printing=1st+Edition+Holofoil&Language=English",
                "https://www.tcgplayer.com/product/45132?ListingType=standard&page=1&Condition=Lightly+Played|Near+Mint&Printing=1st+Edition+Holofoil&Language=English",
                "https://www.tcgplayer.com/product/45143?ListingType=standard&page=1&Condition=Lightly+Played|Near+Mint&Printing=1st+Edition+Holofoil&Language=English",
                ]
        return urls
    except Exception as e:
        logger.error(f"Error getting test URLs: {e}")
        return []
    finally:
        if conn:
            pool.putconn(conn)  # Return connection to pool

def update_values(conn, value_data):
    cursor = conn.cursor()
    for url, value in value_data:
        cursor.execute("""
            UPDATE prize 
            SET value = %s 
            WHERE tcgplayer_url = %s
        """, (value, url))
    conn.commit()

def get_monitor_resolution():
    width, height = pyautogui.size()
    monitors = get_monitors()
    print(monitors)
    logger.info(f"Detected monitor resolution: {width}x{height}")
    return width, height

def initialize_webdriver(instance_num):
    print(f"Starting driver #{instance_num}")
    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")

    try: 
        driver = uc.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print(f"Failed to initialize driver #{instance_num}: {str(e)}")
        raise

def position_to_subquadrant(driver, quadrant):
    logger.debug(f"Positioning to subquadrant {quadrant}")
    screen_width, screen_height = get_monitor_resolution()
    width = screen_width // 2
    height = screen_height // 2
    
    row = (quadrant - 1) // 2
    col = (quadrant - 1) % 2
    
    pos_x = col * width
    pos_y = row * height
    
    try:
        driver.set_window_rect(x=pos_x, y=pos_y, width=width, height=height)
        logger.info(f"Window positioned to subquadrant {quadrant}")
    except Exception as e:
        logger.error(f"Failed to position window: {str(e)}")
    time.sleep(random.uniform(0.5, 1))

def add_failure_tracking_table(pool):
    conn = None
    try:
        conn = pool.getconn()
        cursor = conn.cursor()
        
        # Create table to track failures
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS price_scrape_failures (
                tcgplayer_url TEXT PRIMARY KEY,
                failure_count INTEGER DEFAULT 0,
                last_failure_date DATE,
                consecutive_days INTEGER DEFAULT 0,
                last_success_date DATE
            )
        """)
        conn.commit()
    finally:
        if conn:
            pool.putconn(conn)

def process_url_batch(driver, urls, position):
    """Process a batch of URLs in a single browser window"""
    results = []
    discord_webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
    
    for url in urls:
        conn = None
        try:
            conn = connection_pool.getconn()
            cursor = conn.cursor()
            
            # Try up to 2 times for each URL
            for attempt in range(2):
                try:
                    # Wait for initial page load
                    driver.get(url)
                    time.sleep(1)
                    
                    WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.tcg-standard-button__content')))
                    time.sleep(0.1)

                    listing_elements = WebDriverWait(driver, 20).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.listing-item__listing-data'))
                    )
                    time.sleep(1)
                    logger.info(f"Number of listing elements found: {len(listing_elements)}")

                    listings = driver.find_elements(By.CSS_SELECTOR, '.listing-item__listing-data')
                    logger.info(f"Number of listings after delay: {len(listings)}")
                    prices = []
                    try:
                        price_elements = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".listing-item__listing-data__info__price:not(:empty)"))
                        )
                        print("found elements")

                        price_texts = WebDriverWait(driver, 10).until(
                            lambda x: [el.get_attribute('textContent') for el in price_elements]
                        )

                        for price_text in price_texts:
                            try:
                                price = float(price_text.replace('$', '').replace(',', ''))
                                prices.append(price)
                            except ValueError:
                                print("no price")

                        if prices:
                            mean_price = round(sum(prices) / len(prices), 2) 
                            adjusted_price = round(mean_price * 1.1, 2)
                            
                            # Update price
                            cursor.execute("""
                                UPDATE prize 
                                SET value = %s 
                                WHERE tcgplayer_url = %s
                            """, (adjusted_price, url))
                            
                            # Reset failure tracking on success
                            cursor.execute("""
                                INSERT INTO price_scrape_failures 
                                    (tcgplayer_url, failure_count, last_success_date, consecutive_days) 
                                VALUES (%s, 0, CURRENT_DATE, 0)
                                ON CONFLICT (tcgplayer_url) 
                                DO UPDATE SET 
                                    failure_count = 0,
                                    last_success_date = CURRENT_DATE,
                                    consecutive_days = 0
                            """, (url,))
                            
                            conn.commit()
                            results.append((url, adjusted_price))
                            break  # Success, exit retry loop
                            
                        else:
                            if attempt == 1:  # Final attempt failed
                                # Update failure tracking
                                cursor.execute("""
                                    INSERT INTO price_scrape_failures AS f
                                        (tcgplayer_url, failure_count, last_failure_date, consecutive_days)
                                    VALUES (%s, 1, CURRENT_DATE, 
                                        CASE 
                                            WHEN CURRENT_DATE - INTERVAL '1 day' = 
                                                (SELECT last_failure_date FROM price_scrape_failures WHERE tcgplayer_url = %s)
                                            THEN (SELECT consecutive_days + 1 FROM price_scrape_failures WHERE tcgplayer_url = %s)
                                            ELSE 1
                                        END)
                                    ON CONFLICT (tcgplayer_url) DO UPDATE SET
                                        failure_count = f.failure_count + 1,
                                        last_failure_date = CURRENT_DATE,
                                        consecutive_days = 
                                            CASE 
                                                WHEN CURRENT_DATE - INTERVAL '1 day' = f.last_failure_date
                                                THEN f.consecutive_days + 1
                                                ELSE 1
                                            END
                                """, (url, url, url))
                                
                                # Check if we should send alert
                                cursor.execute("""
                                    SELECT consecutive_days, failure_count 
                                    FROM price_scrape_failures 
                                    WHERE tcgplayer_url = %s
                                """, (url,))
                                consecutive_days, failure_count = cursor.fetchone()
                                
                                if consecutive_days >= 2:
                                    if discord_webhook_url:
                                        message = {
                                            "content": f"⚠️ Card has failed for {consecutive_days} consecutive days: {url}\n"
                                                      f"Total lifetime failures: {failure_count}"
                                        }
                                        try:
                                            requests.post(discord_webhook_url, json=message)
                                        except Exception as e:
                                            logger.error(f"Failed to send Discord notification: {e}")
                                
                                conn.commit()
                                results.append((url, 0))
                            else:
                                time.sleep(random.uniform(1, 2))  # Wait before retry
                                
                    except (TimeoutException, StaleElementReferenceException) as e:
                        if attempt == 1:  # Only log and notify on final attempt
                            logger.error(f"Error scraping prices after retries: {e}")
                                # ... existing error handling code ...
                except Exception as e:
                    if attempt == 1:
                        logger.error(f"Unexpected error during scraping: {e}")
                        # Update failure tracking here
                    
        except Exception as e:
            logger.error(f"Database error: {e}")
        finally:
            if conn:
                connection_pool.putconn(conn)
    
    return results

def cleanup_driver(driver):
    try:
        driver.close()
        time.sleep(0.5)  # Give it a moment
        driver.quit()
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")

def get_problem_cards(pool):
    conn = None
    try:
        conn = pool.getconn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT p.tcgplayer_url, p.name, f.consecutive_days, f.failure_count
            FROM prize p
            JOIN price_scrape_failures f ON p.tcgplayer_url = f.tcgplayer_url
            WHERE f.consecutive_days >= 2
            ORDER BY f.consecutive_days DESC, f.failure_count DESC
        """)
        return cursor.fetchall()
    finally:
        if conn:
            pool.putconn(conn)

def main():
    global connection_pool
    
    # Initialize the connection pool
    connection_pool = initialize_connection_pool()
    if not connection_pool:
        return
        
    # Create failure tracking table if it doesn't exist
    add_failure_tracking_table(connection_pool)
    
    # Get monitor resolution once at the start
    screen_width, screen_height = get_monitor_resolution()
    
    # Get test URLs
    urls = get_test_urls(connection_pool)
    print(f"Retrieved {len(urls)} URLs to process")
    
    drivers = []
    all_results = []
    
    try:
        # Initialize 2 drivers (no VPN)
        for i in range(1, 2):
            driver = initialize_webdriver(i)
            position_to_subquadrant(driver, i)
            drivers.append(driver)
            time.sleep(2)
        
        # Divide URLs into 2 chunks (instead of 4)
        driver_url_chunks = [urls[i::1] for i in range(1)]
        
        # Process URLs with each driver working independently
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = []
            for idx, (driver, url_chunk) in enumerate(zip(drivers, driver_url_chunks)):
                time.sleep(3)
                futures.append(
                    executor.submit(
                        process_url_batch,
                        driver,
                        url_chunk,
                        idx + 1
                    )
                )
            
            # Process results as they complete
            for future in as_completed(futures):
                try:
                    results = future.result()
                    all_results.extend(results)
                except Exception as e:
                    print(f"Error in batch processing: {e}")
    finally:
        # Cleanup
        for driver in drivers:
            cleanup_driver(driver)
        
        # Clean up the connection pool
        if connection_pool:
            connection_pool.closeall()
            logger.info("Connection pool closed")

if __name__ == "__main__":
    main()
    
