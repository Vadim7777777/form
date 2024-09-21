import requests
from bs4 import BeautifulSoup
import csv
import mysql.connector


def save_to_db(data, db_config):
    # Connect to the MySQL database
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Create a new table for storing links (if it doesn't exist)
    cursor.execute('''CREATE TABLE IF NOT EXISTS voz.vn.links (
                      id INT AUTO_INCREMENT PRIMARY KEY,
                      url TEXT NOT NULL,
                      text TEXT)''')

    # Insert the data into the database
    insert_query = 'INSERT INTO links (url, text) VALUES (%s, %s)'
    cursor.executemany(insert_query, data)

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()


def fetch_and_parse(url):
    try:
        # Define headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        # Fetch the webpage content with headers
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError if the response was an error
        # Parse the content with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None


def extract_links(soup):
    # Extract title text and the href attribute from anchor tags
    # Filter to include only those URLs that start with "https://voz.vn/f/"
    return [("https://voz.vn" + a['href'], a.text.strip()) for a in soup.find_all('a', href=True) if a['href'].startswith("/f/")]

def save_to_csv(data, filename="links.csv"):
    # Save the extracted data to a CSV file
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Text", "URL"])  # Header
        writer.writerows(data)

if __name__ == "__main__":
    url = "https://voz.vn/"  # Change to the target webpage
    soup = fetch_and_parse(url)
    if soup:
        links = extract_links(soup)
        save_to_csv(links)
        print(f"Data has been saved to links.csv")