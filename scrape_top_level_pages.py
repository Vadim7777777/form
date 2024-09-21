import csv
import logging
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

def extract_f_parameter(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    f_value = query_params.get('f', [None])[0]
    return int(f_value) if f_value and f_value.isdigit() else None

def scrape_webpage(url, writer, visited_urls, base_url):
    try:
        final_url = clean_url(url)
        if final_url in visited_urls:
            logging.debug("Already visited URL: %s", final_url)
            return

        visited_urls.add(final_url)
        response = requests.get(final_url)
        response.raise_for_status()
        tree = html.fromstring(response.content)
        links = tree.xpath('//a[contains(@href, "forumdisplay.php")]')

        for link in links:
            href = urljoin(base_url, link.get('href'))
            final_href = clean_url(href)
            text = link.text_content().strip()

            if final_href not in visited_urls:
                writer.writerow([text, final_href])
                visited_urls.add(final_href)
                logging.debug("Processed and written to CSV: %s, %s", text, final_href)
                scrape_webpage(final_href, writer, visited_urls, base_url)

    except requests.RequestException as e:
        logging.error("Error fetching the webpage: %s", e)
    except Exception as e:
        logging.error("An error occurred: %s", e)

def main():
    webpage_url = "https://www.linustechtips.com"
    output_filename = "top_level_links_forumhr.csv"

    with open(output_filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Text', 'URL'])
        visited_urls = set()

        logging.info("Starting the scraping process.")
        scrape_webpage(webpage_url, writer, visited_urls, webpage_url)
        logging.info("Scraping process completed.")

    print(f"Data successfully written to {output_filename}")

if __name__ == "__main__":
    main()