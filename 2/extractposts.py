from urllib.parse import urlparse, urlencode, parse_qs, urlunparse, urljoin
import pymysql
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import concurrent.futures

import logging

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters
db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '12345',
    'database': 'forumhr2',
    'port': 3310
}


# Connect to the MySQL database


def fetch_urls():
    connection = pymysql.connect(host=db_config['host'],
                                 user=db_config['user'],
                                 password=db_config['password'],
                                 database=db_config['database'],
                                 port=db_config['port'],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection.cursor() as cursor:
        sql = "SELECT id, url, pages FROM threads where scraped is null"
        cursor.execute(sql)
        return cursor.fetchall()


def clean_url(link):
    parsed_link = urlparse(link)
    query_parts = parse_qs(parsed_link.query)
    query_parts.pop('s', None)
    cleaned_query = urlencode(query_parts, doseq=True)
    cleaned_link = urlunparse(parsed_link._replace(query=cleaned_query))
    return cleaned_link

def safe_encode(text):
    try:
        # Try to encode normally
        encoded_text = text.encode('utf-8')
        print("Encoded successfully:", encoded_text)
    except UnicodeEncodeError as e:
        print("Error:", str(e))
        print("Fallback: encode as UTF-16 with explicit handling for surrogate pairs")
        encoded_text = text.encode('utf-8', 'surrogatepass')
        print("Encoded with surrogatepass:", encoded_text)
    return encoded_text


def insert_post(data):
    connection = pymysql.connect(host=db_config['host'],
                                 user=db_config['user'],
                                 password=db_config['password'],
                                 database=db_config['database'],
                                 port=db_config['port'],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection.cursor() as cursor:
        sql = """
        INSERT IGNORE INTO posts (text, thread_id, author, post_date, 
            post_num, signature, post_link, registration_date, error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.execute(sql, (
                data['post_message'], data['thread_id'], data['username'], data['post_date'], data['post_number'],
                data['signature'], data['post_link'], data['registration_date'], data['error'],))
            connection.commit()
        except Exception as e:
            logging.info(f"Error inserting into DB: {e}")
            logging.info(f"Reinsert: {e}")
            cursor.execute(sql, (
                safe_encode(data['post_message']), data['thread_id'], safe_encode(data['username']), data['post_date'], data['post_number'],
                safe_encode(data['signature']), data['post_link'], data['registration_date'], data['error'],))
            connection.commit()
            logging.info(f"Successfully reinserting into DB: {e}")


def parse_date(date_text):
    now = datetime.now()
    time_str = date_text.split(',')[-1].strip()  # Extracting the time part from the date text

    if "Jučer" in date_text:
        date_part = now - timedelta(days=1)  # Calculating 'yesterday's date
    else:
        try:
            # Attempting to parse the full date if not a relative term
            return datetime.strptime(date_text, '%d.%m.%Y., %H:%M').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            date_part = now  # Default to current time if parsing fails

    # Combine the date part with the extracted time part
    full_datetime = datetime.combine(date_part.date(), datetime.strptime(time_str, '%H:%M').time())
    return full_datetime.strftime('%Y-%m-%d %H:%M:%S')


def parse_date(date_text):
    now = datetime.now()
    time_str = date_text.split(',')[-1].strip()  # Extracting the time part from the date text

    if "Jučer" in date_text:
        date_part = now - timedelta(days=1)  # Calculating 'yesterday's date
    else:
        try:
            # Attempting to parse the full date if not a relative term
            return datetime.strptime(date_text, '%d.%m.%Y., %H:%M').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            date_part = now  # Default to current time if parsing fails

    # Combine the date part with the extracted time part
    full_datetime = datetime.combine(date_part.date(), datetime.strptime(time_str, '%H:%M').time())
    return full_datetime.strftime('%Y-%m-%d %H:%M:%S')


def scrape_forum_page(url, thread_id, post_selector, post_num_set, same_counter, page):
    # headers = {
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    #     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    #     'Accept-Language': 'en-US,en;q=0.5',
    #     'Accept-Encoding': 'gzip, deflate, br',
    #     'Connection': 'keep-alive',
    #     'Upgrade-Insecure-Requests': '1',
    #     'Cache-Control': 'max-age=0'
    # }
    #
    # cookies = {'bbsessionhash':	'9565bfb707c8b0e623b1bfb75bfd6966',
    #             'bbpassword':	'db2c63d9701d9a0723c3d0ba1f709ed9',
    #             'bblastactivity':	'0',
    #             'bblastvisit':	'1714265949'}
    s = requests.Session()
    # for name, value in cookies.items():
    #     s.cookies.set(name, value, domain=".linustechtips.com", path="/")
    #response = s.get(url, headers=headers)
    # response = s.get(url)

    response = requests.get(
        url,
        proxies={
            "http": "http://6caac9c8ec704c9e9e32b7518494bb34:@api.zyte.com:8011/",
            "https": "http://6caac9c8ec704c9e9e32b7518494bb34:@api.zyte.com:8011/",
        },
        verify='zyte-ca.crt'
    )

    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    posts = soup.find_all('table', id=lambda x: x and x.startswith('post') and x[4:].isdigit())
    result = []

    for post in posts:
        try:
            error = 0
            post_number_element = post.find(name="a", id=lambda x: x and x.startswith('postcount'))
            error = 1
            post_link = post_number_element['href']
            error = 2
            post_number = post_number_element.text
            error = 3
            username_block = post.find(class_="bigusername")
            if username_block is not None:
                username = username_block.get_text(strip=True)
            else:
                username_block = post.find(name='div', id=lambda x: x and x.startswith('postmenu') and x[9:].isdigit())
                username = username_block.get_text(strip=True)
            error = 4
            post_message = post.find(id=lambda x: x and x.startswith('post_message_')).get_text(strip=False)
            error = 5

            post_link = clean_url(post_link)
            error = 6
            signature = post.find('div', class_='signature').get_text(strip=True) if post.find('div',
                                                                                               class_='signature') else ''
            error = 7
            date_container = post.find('td', class_='thead')
            error = 8
            date_text = date_container.get_text(strip=True)
            error = 9
            formatted_date = parse_date(date_text)
            error = 10
            reg_date_container = post.find('div', text=lambda x: x and 'Registracija' in x)
            error = 11
            registration_date = reg_date_container.text.split(':')[-1].strip() if reg_date_container else 'Unknown'
            error = 12
            # Format registration date (example: Jun 2010) if needed
            # If you want to store it as a date, you might need to parse it accordingly, here just stored as string
            formatted_registration_date = datetime.strptime(registration_date, '%b %Y.').strftime(
                '%Y-%m-%d %H:%M:%S') if 'Unknown' not in registration_date else 'Unknown'
            error = 13
            # post_date = datetime.strptime(date_text, '%d.%m.%Y., %H:%M')
            # formatted_date = post_date.strftime('%Y-%m-%d %H:%M:%S')

            data = {
                'username': username,
                'post_message': post_message,
                'post_number': post_number,
                'post_link': urljoin('https://www.linustechtips.com/', post_link),
                'thread_id': thread_id,
                'post_date': formatted_date,
                'registration_date': formatted_registration_date,
                'signature': signature,
                'error': error
            }
            post_num_set.add(post_number)
            print(f"Same counter for thread {thread_id} is {same_counter}. Current page is {page}\r\n")
            insert_post(data)
            # result.append(data)
            logging.info("New post scraped: %s", data)
        except AttributeError as e:
            error = 14
            data = {
                'username': "",
                'post_message': "",
                'post_number': "",
                'post_link': urljoin('https://www.linustechtips.com/', post_link),
                'thread_id': thread_id,
                'post_date': "",
                'registration_date': "",
                'signature': "",
                'error': error
            }
            insert_post(data)
            logging.info(f"Error parsing post data: {thread_id}, {error}")
            continue

    return len(posts)


def func(item):
    base_url = item['url']
    num_pages = item['pages']
    thread_id = item['id']

    connection = pymysql.connect(host=db_config['host'],
                                 user=db_config['user'],
                                 password=db_config['password'],
                                 database=db_config['database'],
                                 port=db_config['port'],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection.cursor() as cursor:
        print("Finding max page")
        select_page = "select floor(max(post_num - 1)/20)+1 as max_post_num from forumhr2.posts where thread_id = (%s)"
        cursor.execute(select_page, (thread_id,))
        initial_page = cursor.fetchone()
        initial_page = initial_page['max_post_num']
        print("Max page found")

    # if page_max == num_pages:
    #     logging.info(f"Skipping thread {thread_id} because it was scraped")
    #     logging.info(f"Marking thread {thread_id} as scraped")
    #
    #     with connection.cursor() as cursor:
    #         thread_scraped_query = "UPDATE forumhr2.threads SET scraped = 1 WHERE id = (%s)"
    #         cursor.execute(thread_scraped_query, (thread_id,))
    #         connection.commit()
    #     return
    if initial_page is None:
        initial_page = 1
    #TODO: time data 'Registracija istekla' does not match format '%b %Y.'
    same_counter = -1
    same_posts_set = set()
    same_counter_prev = -1
    for page in range(initial_page, 500 + 1):
        print(f"visiting {page} of thread {thread_id}")
        paginated_url = f"{base_url}&page={page}"
        new_post_count = scrape_forum_page(paginated_url, thread_id, 'table.tborder', same_posts_set, same_counter, page)
        if new_post_count == 0:
            break
        if same_counter == same_counter_prev and same_counter > 0:
            print(f"Same counter stopped finding new posts for thread id {thread_id}")
            break
        same_counter_prev = same_counter
        same_counter = len(same_posts_set)
        print(f"Same counter for {thread_id} : {same_counter}")
    with connection.cursor() as cursor:
        thread_scraped_query = "UPDATE forumhr2.threads SET scraped = (%s) WHERE id = (%s)"
        cursor.execute(thread_scraped_query, (page, thread_id,))
        connection.commit()
        print(f"Marking thread number {thread_id} as scraped.")
    return True


# def thread_function(item):
#
def main():
    print("Fetching URLS")
    urls = fetch_urls()
    print("Fetching URLS done")

    all_posts = []


    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        pool.map(func, urls)


if __name__ == "__main__":
    main()
