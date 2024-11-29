import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

def setup_headless_driver():
    options = uc.ChromeOptions()
    options.add_argument('--headless=new')  # Using the new headless mode
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    return uc.Chrome(options=options)

def scrape_tcg_prices(urls):
    driver = setup_headless_driver()
    prices = []
    
    try:
        for url in urls:
            driver.get(url)
            
            # Wait for price elements
            price_elements = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, ".listing-item__listing-data__info__price:not(:empty)")
                )
            )
            print(f"Found {len(price_elements)} price elements")

            # Extract prices
            price_texts = [el.get_attribute('textContent') for el in price_elements]
            
            for price_text in price_texts:
                try:
                    price = float(price_text.replace('$', '').replace(',', ''))
                    prices.append(price)
                    print(f"Processed price: ${price:.2f}")
                except ValueError:
                    print(f"Could not process price text: {price_text}")
        
        if prices:
            mean_price = sum(prices) / len(prices)
            print(f"\nMean price: ${mean_price:.2f}")
        else:
            print("\nNo valid prices found")
            
    except Exception as e:
        print(f"Error during scraping: {e}")
    finally:
        driver.quit()
        
    return prices

if __name__ == "__main__":
    urls = ["https://www.tcgplayer.com/product/45123?ListingType=standard&page=1&Condition=Lightly+Played|Near+Mint&Printing=1st+Edition+Holofoil&Language=English",
            "https://www.tcgplayer.com/product/45132?ListingType=standard&page=1&Condition=Lightly+Played|Near+Mint&Printing=1st+Edition+Holofoil&Language=English",
            "https://www.tcgplayer.com/product/45143?ListingType=standard&page=1&Condition=Lightly+Played|Near+Mint&Printing=1st+Edition+Holofoil&Language=English",
            ]
    scrape_tcg_prices(urls)
