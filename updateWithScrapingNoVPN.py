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
import csv

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

def get_test_urls(pool):
    conn = None
    try:
        conn = pool.getconn()  # Get connection from pool
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT tcgplayer_url 
            FROM prize 
            WHERE tcgplayer_url IS NOT NULL 
            AND is_deleted = false
            AND is_manually_priced = false
        """)
        urls = [row[0] for row in cursor.fetchall()]
        logger.info(f"Retrieved {len(urls)} unique URLs")
        return urls
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

def handle_retry_logic(url, error, retry_count, discord_webhook_url, results):
    """Helper function to handle retry logic and Discord notifications"""
    if retry_count == 2:  # Only notify on final attempt
        if discord_webhook_url:
            message = {"content": f"Failed to process card after {retry_count} attempts: {url}\nError: {str(error)}"}
            try:
                requests.post(discord_webhook_url, json=message)
                add_count_csv(url)
            except Exception as e:
                logger.error(f"Failed to send Discord notification: {e}")
        logger.error(f"Error processing {url}: {error}")
        results.append((url, None))
    else:
        time.sleep(random.uniform(1, 2))  # Wait before retry

def add_count_csv(url):
    """Track failed URLs and their failure counts in a CSV"""
    csv_file = 'failed_products.csv'
    failed_webhook_url = os.getenv('FAILED_WEBHOOK')
    
    logger.info(f"Attempting to add/update count for URL: {url}")
    
    # Create file with headers if it doesn't exist
    if not os.path.exists(csv_file):
        logger.info("CSV file doesn't exist, creating new one")
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['product_url', 'count'])
    
    # Read existing data
    urls_count = {}
    with open(csv_file, 'r', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            urls_count[row['product_url']] = int(row['count'])
    
    logger.info(f"Current URL counts before update: {urls_count}")
    
    # Update count for URL
    if url in urls_count:
        urls_count[url] += 1
        logger.info(f"Incremented count for {url} to {urls_count[url]}")
    else:
        urls_count[url] = 1
        logger.info(f"Added new URL {url} with count 1")
    
    # Write updated data back to CSV
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['product_url', 'count'])
        for url, count in urls_count.items():
            writer.writerow([url, count])
    
    logger.info(f"Final URL counts after update: {urls_count}")
    if urls_count[url] >= 3 and failed_webhook_url:
        try:
            conn = connection_pool.getconn()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT p.image, p.name, b.name as box_name 
                FROM prize p 
                JOIN box b ON b.id = p.box_id 
                WHERE p.tcgplayer_url = %s
            """, (url,))
            
            card_instances = cursor.fetchall()
            
            # Format the message with card details
            message_content = f"⚠️ Critical: URL has failed 3 or more times:\n{url}\n\n"
            
            if card_instances:  # Check if we got any results
                message_content += f"Card: {card_instances[0][1]}\n"  # First instance name
                message_content += f"Image: {card_instances[0][0]}\n\n"  # First instance image
                message_content += "This card appears in the following boxes:\n"
                
                for _, _, box_name in card_instances:
                    message_content += f"• {box_name}\n"
            else:
                message_content += "No card details found in database for this URL"
                
            message = {"content": message_content}
            
            requests.post(failed_webhook_url, json=message)
            
        except Exception as e:
            logger.error(f"Failed to send critical failure notification: {e}")
        finally:
            if conn:
                connection_pool.putconn(conn)

def process_url_batch(driver, urls, position):
    """Process a batch of URLs in a single browser window"""
    results = []
    discord_webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
    
    for url in urls:
        conn = None
        retry_count = 0
        
        while retry_count < 2:
            try:
                conn = connection_pool.getconn()
                
                # Wait for initial page load
                driver.get(url)
                time.sleep(1)
                
                WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.tcg-standard-button__content')))
                time.sleep(0.1)

                listing_elements = WebDriverWait(driver, 10).until(
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
                        # Success! Update price and break the retry loop
                        mean_price = round(sum(prices) / len(prices), 2) 
                        adjusted_price = round(mean_price * 1.1, 2)
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE prize 
                            SET value = %s 
                            WHERE tcgplayer_url = %s
                        """, (adjusted_price, url))
                        conn.commit()
                        results.append((url, adjusted_price))
                        logger.info(f"Processed and updated URL: {url} (Original: ${mean_price}, Adjusted: ${adjusted_price})")
                        break  # Exit retry loop on success
                    else:
                        retry_count += 1  # Increment retry count
                        if retry_count == 2:  # Only notify on final attempt
                            if discord_webhook_url:
                                message = {"content": f"No prices found for card after {retry_count} attempts: {url}"}
                                try:
                                    requests.post(discord_webhook_url, json=message)
                                except Exception as e:
                                    logger.error(f"Failed to send Discord notification: {e}")
                            results.append((url, None))
                        else:
                            time.sleep(random.uniform(1, 2))  # Wait before retry
                
                except (TimeoutException, StaleElementReferenceException) as e:
                    retry_count += 1
                    handle_retry_logic(url, e, retry_count, discord_webhook_url, results)
                    
            except Exception as e:
                retry_count += 1
                handle_retry_logic(url, e, retry_count, discord_webhook_url, results)
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

def main():
    global connection_pool
    
    # Initialize the connection pool
    connection_pool = initialize_connection_pool()
    if not connection_pool:
        return
    
    # Get monitor resolution once at the start
    screen_width, screen_height = get_monitor_resolution()
    
    # Get test URLs
    urls = get_test_urls(connection_pool)
    print(f"Retrieved {len(urls)} URLs to process")
    
    drivers = []
    all_results = []
    
    try:
        # Initialize 2 drivers (no VPN)
        for i in range(1, 3):
            driver = initialize_webdriver(i)
            position_to_subquadrant(driver, i)
            drivers.append(driver)
            time.sleep(2)
        
        # Divide URLs into 2 chunks (instead of 4)
        driver_url_chunks = [urls[i::2] for i in range(2)]
        
        # Process URLs with each driver working independently
        with ThreadPoolExecutor(max_workers=2) as executor:
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
    
