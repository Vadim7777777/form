import csv
import logging
import re
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode, urljoin
import requests
from lxml import html

# Setup logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_url(url):
    """
    Cleans the URL by keeping only the 'f' parameter. If 'f' is not present,
    follows the URL to see where it redirects and returns the final URL.

    Args:
    url (str): The original URL.

    Returns:
    str: The URL with only the 'f' parameter or the final redirect URL if 'f' is absent.
    """
    url_parts = list(urlparse(url))
    query = dict(parse_qs(url_parts[4], keep_blank_values=True))
    f_value = query.get('f', [None])[0]
    if isinstance(f_value, list):  # Checking if the extracted value is a list
        f_value = f_value[0]       # Take the first element if it is a list

    if f_value:
        # Ensure that 'f' values are properly cleaned up and encoded
        url_parts[4] = urlencode({'f': f_value}, doseq=True)
    else:
        # Follow redirects if 'f' is absent
        response = requests.get(url, allow_redirects=True)
        url_parts = list(urlparse(response.url))  # Update url_parts with the redirected URL
        query = dict(parse_qs(url_parts[4], keep_blank_values=True))
        f_value = query.get('f', [None])[0]
        if f_value:
            url_parts[4] = urlencode({'f': f_value}, doseq=True)

    return urlunparse(url_parts)


def login(session, login_url, username, password):
    """
    Perform login to the site and return the session with authentication cookies.

    Args:
    session (requests.Session): The session object.
    login_url (str): The URL for the login form.
    username (str): The username for login.
    password (str): The password for login.

    Returns:
    requests.Session: The session after authentication.
    """
    login_data = {
        'username': username,
        'password': password
    }
    try:
        response = session.post(login_url, data=login_data)
        response.raise_for_status()
        if "Login failed" in response.text:
            logging.error("Login failed, check username and password")
            return None
        logging.debug("Logged in successfully")
    except requests.RequestException as e:
        logging.error("Login failed: %s", e)
        return None
    return session

def extract_f_parameter(url):
    match = re.search(r"[?&]f=(\d+)", url)
    if match:
        return int(match.group(1))
    else:
        return None

def scrape_webpage(session, url, writer, visited_urls):
    try:
        f_id = extract_f_parameter(url)

        if f_id in visited_urls:
            logging.debug("Already visited URL: %s", url)
            return
        visited_urls.add(f_id)
        visited_urls.add(url)

        writer.writerow([url])

        response = session.get(url)  # Use session to maintain login state
        response.raise_for_status()

        tree = html.fromstring(response.content)
        links = tree.xpath('//a[contains(@href, "forumdisplay.php")]')
        relevant_links = [link for link in links if 'forumdisplay.php' in link.get('href', '')]

        for link in relevant_links:
            href = urljoin("https://www.linustechtips.com", link.get('href'))
            f_id = extract_f_parameter(href)
            if f_id in visited_urls:
                continue
            text = link.text.strip() if link.text else ''
            writer.writerow([href])
            visited_urls.add(href)
            scrape_webpage(session, href, writer, visited_urls)

    except requests.RequestException as e:
        logging.error("Error fetching the webpage: %s", e)
    except Exception as e:
        logging.error("An error occurred: %s", e)


def main():
    login_url = "https://www.linustechtips.com/login.php"  # Adjust if necessary
    webpage_url = "https://www.linustechtips.com"
    output_filename = "top_level_links_forumhr.csv"
    username = "wavepunk"
    password = "1q2w3e4r"

    session = requests.Session()
    if login(session, login_url, username, password):
        with open(output_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Text', 'URL'])
            visited_urls = set()
            scrape_webpage(session, webpage_url, writer, visited_urls)
            logging.info("Scraping process completed.")
        print(f"Data successfully written to {output_filename}")
    else:
        print("Failed to log in and scrape data.")

if __name__ == "__main__":
    main()