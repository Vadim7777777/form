import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent

def configure_driver():
    # Generate a random realistic user agent
    ua = UserAgent()
    user_agent = ua.random
    print("Using User-Agent:", user_agent)

    # Configure Chrome options
    options = Options()
    options.add_argument(f"user-agent={user_agent}")

    # Initialize the Chrome driver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def wait_for_cloudflare(driver):
    # Wait for Cloudflare's JS check to pass
    try:
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.ID, "some_element_id_after_cloudflare")))
        print("Cloudflare delay passed")
    except Exception as e:
        print("Waiting for Cloudflare failed:", e)

def login(driver, url, username, password):
    # Navigate to login page
    driver.get(url)
    wait_for_cloudflare(driver)

    # Input login credentials and submit
    driver.find_element(By.ID, 'username_field').send_keys(username)
    driver.find_element(By.ID, 'password_field').send_keys(password)
    driver.find_element(By.ID, 'login_button').click()

    # Wait for login to be processed and check if it's successful
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "element_visible_after_login")))

def scrape_data(driver):
    # Replace this function with the actual data scraping logic
    data = driver.find_element(By.ID, 'data_element').text
    print("Scraped data:", data)
    return data

def main():
    driver = configure_driver()
    login_url = 'https://linustechtips.com/'
    username = 'wavepunk'
    password = '1q2w3e4r'

    # Perform login
    login(driver, login_url, username, password)

    # Navigate to the page to scrape
    driver.get('https://linustechtips.com/')

    # Scrape data
    scraped_data = scrape_data(driver)

    # Get cookies if needed
    cookies = driver.get_cookies()
    print("Cookies obtained:", cookies)

    # Cleanup
    driver.quit()

if __name__ == "__main__":
    main()
