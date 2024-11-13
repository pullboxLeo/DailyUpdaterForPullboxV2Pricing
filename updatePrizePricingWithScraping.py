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
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def load_retool_db():
    load_dotenv()
    database_url = os.getenv('PRODUCTION_DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL not found in .env file")
        return

    parsed_url = urlparse(database_url)
    dbname = parsed_url.path[1:]  
    user = parsed_url.username
    password = parsed_url.password
    host = parsed_url.hostname
    port = parsed_url.port or 5432 

    try:
        retool_conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            sslmode='require'
        )
        logger.info("Connected to the database successfully!")
        return retool_conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to the database: {e}")
        return None

def get_test_urls(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT tcgplayer_url 
        FROM prize 
        WHERE tcgplayer_url IS NOT NULL 
        LIMIT 320
    """)
    urls = [row[0] for row in cursor.fetchall()]
    logger.info(f"Retrieved {len(urls)} unique URLs")
    return urls

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

def initialize_webdriver():
    logger.debug("Initializing webdriver")
    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")

    prefs = {
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        "profile.default_content_setting_values.notifications": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)

    try: 
        driver = uc.Chrome(options=chrome_options)
        logger.info("Webdriver initialized successfully")
        return driver
    except Exception as e:
        logger.error(f"Failed to initialize webdriver: {str(e)}")
        raise

def position_to_subquadrant(driver, quadrant):
    logger.debug(f"Positioning to subquadrant {quadrant}")
    screen_width, screen_height = get_monitor_resolution()
    width = screen_width // 4
    height = screen_height // 4
    
    row = (quadrant - 1) // 4
    col = (quadrant - 1) % 4
    
    pos_x = col * width
    pos_y = row * height
    
    try:
        driver.set_window_rect(x=pos_x, y=pos_y, width=width, height=height)
        logger.info(f"Window positioned to subquadrant {quadrant}")
    except Exception as e:
        logger.error(f"Failed to position window: {str(e)}")
    time.sleep(random.uniform(0.5, 1))

def process_url_batch(driver, urls, position):
    """Process a batch of URLs in a single browser window"""
    results = []
    discord_webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
    
    for url in urls:
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
                    results.append((url, mean_price))
                    logger.info(f"Processed URL: {url}")
                else:
                    # No prices found - notify Discord and continue
                    if discord_webhook_url:
                        message = {"content": f"No prices found for card: {url}"}
                        try:
                            requests.post(discord_webhook_url, json=message)
                        except Exception as e:
                            logger.error(f"Failed to send Discord notification: {e}")
                    results.append((url, 0))  # Add with 0 price instead of failing
            
            except (TimeoutException, StaleElementReferenceException) as e:
                if discord_webhook_url:
                    message = {"content": f"Failed to scrape card: {url}\nError: {str(e)}"}
                    try:
                        requests.post(discord_webhook_url, json=message)
                    except Exception as e:
                        logger.error(f"Failed to send Discord notification: {e}")
                logger.error(f"Error scraping prices: {e}")
                results.append((url, 0))
                
        except Exception as e:
            if discord_webhook_url:
                message = {"content": f"Failed to process card: {url}\nError: {str(e)}"}
                try:
                    requests.post(discord_webhook_url, json=message)
                except Exception as e:
                    logger.error(f"Failed to send Discord notification: {e}")
            logger.error(f"Error processing {url}: {e}")
            results.append((url, 0))
    
    return results

def cleanup_driver(driver):
    try:
        driver.close()
        time.sleep(0.5)  # Give it a moment
        driver.quit()
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")

def main():
    # Get monitor resolution once at the start
    screen_width, screen_height = get_monitor_resolution()
    
    # Initialize database connection
    conn = load_retool_db()
    if not conn:
        return
    
    # Get test URLs
    urls = get_test_urls(conn)
    
    # Initialize drivers
    drivers = []
    try:
        # Initialize 16 drivers and position them once
        for i in range(1, 17):
            driver = initialize_webdriver()
            position_to_subquadrant(driver, i)  # Position each driver once
            drivers.append(driver)
            time.sleep(2)  # Stagger driver creation
        
        # Process URLs in batches of 16
        all_results = []
        for i in range(0, len(urls), 16):
            batch_urls = urls[i:i+16]
            logger.info(f"Processing batch {i//16 + 1} of {(len(urls) + 16 - 1)//16}")
            
            # Split batch among available drivers
            with ThreadPoolExecutor(max_workers=len(batch_urls)) as executor:
                futures = []
                for idx, url_subset in enumerate(batch_urls):
                    driver = drivers[idx]
                    futures.append(
                        executor.submit(
                            process_url_batch, 
                            driver, 
                            [url_subset], 
                            idx + 1
                        )
                    )
                
                # Collect results
                for future in as_completed(futures):
                    try:
                        results = future.result()
                        all_results.extend(results)
                    except Exception as e:
                        logger.error(f"Error in batch processing: {e}")
            
            # Update database after each batch
            update_values(conn, all_results)
            all_results = []  # Clear results after updating
            
    finally:
        # Cleanup
        for driver in drivers:
            cleanup_driver(driver)
        
        if conn:
            conn.close()
        logger.info("Cleanup completed")

if __name__ == "__main__":
    main()
    
