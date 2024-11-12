from selenium import webdriver
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException


driver = uc.Chrome()
driver.get("https://www.tcgplayer.com/product/1042?Language=English&Condition=Near+Mint|Heavily+Played&page=1&Printing=Normal")

try:
    # Wait for initial page load
    time.sleep(1)
    time.sleep(.5)
    
    prices = []
    try:
        # Use a more specific wait condition
        price_elements = WebDriverWait(driver, 8).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".listing-item__listing-data__info__price:not(:empty)"))
        )
        print("found elements")

        # Get all prices in one go
        price_texts = WebDriverWait(driver, 8).until(
            lambda x: [el.get_attribute('textContent') for el in price_elements]
        )

        # Process the stored text values
        for price_text in price_texts:
            print(price_text)
            try:
                price = float(price_text.replace('$', '').replace(',', ''))
                prices.append(price)
            except ValueError:
                print("no price")

    except (TimeoutException, StaleElementReferenceException) as e:
        print(f"Error scraping prices: {e}")

    # Calculate and print mean if we have prices
    if prices:
        mean_price = sum(prices) / len(prices)
        print(f"\nMean price: ${mean_price:.2f}")
    else:
        print("\nNo valid prices found")

finally:
    driver.quit()