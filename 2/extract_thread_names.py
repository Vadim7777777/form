import time

import pymysql
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode, urljoin


def clean_url(link):
    """Remove session id (s parameter) from URLs"""
    parsed_link = urlparse(link)
    query_parts = parse_qs(parsed_link.query)
    # Remove 's' session parameter if it exists
    query_parts.pop('s', None)
    # Rebuild the query string without 's' parameter
    cleaned_query = urlencode(query_parts, doseq=True)
    # Construct the final cleaned URL
    cleaned_link = urlunparse(parsed_link._replace(query=cleaned_query))
    return cleaned_link


# Database connection setup
connection = pymysql.connect(host='127.0.0.1',
                             user='root',
                             password='12345',
                             db='forumhr2',
                             charset='utf8mb4',
                             port=3310,
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        # Fetch all the entries and their page counts from the categories table
        cursor.execute("SELECT id, url, pages FROM categories where scraped is null")
        categories = cursor.fetchall()

        for category in categories:
            category_id = category['id']
            base_url = category['url']
            num_pages = category['pages']

            delay = 30

            for page_number in range(1, num_pages + 1):
                if num_pages > 1:
                    url = f"{base_url}&page={page_number}"
                else:
                    url = base_url
                print(f"Processing {url}")

                while True:
                    try:
                        response = requests.get(url)
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(delay)
                        delay += 30
                        if delay > 90:
                            delay = 90

                soup = BeautifulSoup(response.text, 'html.parser')

                thread_containers = soup.find_all('td', class_='alt1 field_title')
                for container in thread_containers:
                    title_link = container.find('a', id=lambda x: x and x.startswith('thread_title_'))
                    if title_link:
                        title = title_link.text.strip()
                        link = clean_url(title_link['href'])

                        last_page_number = 1
                        last_page_link = container.find('a', text='Zadnja stranica')
                        if last_page_link:
                            href = last_page_link['href']
                            page_param = href.split('page=')[-1]
                            if page_param.isdigit():
                                last_page_number = int(page_param)
                            else:
                                print(f"Error parsing page number from link: {href}")

                        link = clean_url(link)
                        if not 'https://www.linustechtips.com' in link:
                            link = urljoin("https://www.linustechtips.com", link)

                        print(f"Title: {title}, URL: {link}, Last Page Number: {last_page_number}")
                        insert_query = "INSERT IGNORE INTO threads (title, url, pages, category_reference) VALUES (%s, %s, %s, %s)"
                        cursor.execute(insert_query, (title, link, last_page_number, category_id))

                connection.commit()

            category_scraped_sql = "update categories set scraped = 1 where id = (%s)"
            cursor.execute(category_scraped_sql, (category_id,))
            connection.commit()

finally:
    connection.close()
