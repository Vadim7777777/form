import pymysql
import requests
from bs4 import BeautifulSoup

# Database Configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '12345',
    'db': 'forumhr2',
    'charset': 'utf8mb4',
    'port': 3310,
    'cursorclass': pymysql.cursors.DictCursor
}

# Connect to the database
connection = pymysql.connect(**db_config)

try:
    with connection.cursor() as cursor:
        # Read URLs from the database
        sql = "SELECT id, url FROM categories where pages is null"
        cursor.execute(sql)
        urls = cursor.fetchall()

        for entry in urls:
            url_id = entry['id']
            url = entry['url']

            try:
                # Fetch the webpage
                response = requests.get(url)
                response.raise_for_status()  # Check if the request was successful

                # Parse the HTML content
                soup = BeautifulSoup(response.text, 'html.parser')

                # Find the desired element and extract the last number
                element = soup.find('td', class_='vbmenu_control', style='font-weight:normal')
                if element:
                    text = element.get_text()
                    last_number = int(text.split()[-1])  # Assuming the last word is the number

                    # Update the database with the extracted number
                    update_sql = "UPDATE categories SET pages = %s WHERE id = %s"
                    cursor.execute(update_sql, (last_number, url_id))
                    connection.commit()
            except Exception as e:
                print(f"Failed to process URL {url}: {e}")
finally:
    connection.close()