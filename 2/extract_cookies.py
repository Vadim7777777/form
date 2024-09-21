import sqlite3
import requests

# Path to the cookies.sqlite file in your Firefox profile
cookies_path = r'C:\Users\user\AppData\Roaming\Mozilla\Firefox\Profiles\kgvyq348.default-release-1691854931212\cookies.sqlite'

# Connect to the SQLite database
conn = sqlite3.connect(cookies_path)

# Create a cursor object using the cursor method
cursor = conn.cursor()

# Select all the cookies
cursor.execute("SELECT host, name, value FROM moz_cookies WHERE host LIKE '%linustechtips.com%'")

# Fetch all results
cookie_data = cursor.fetchall()

# Close the connection
cursor.close()
conn.close()

# Prepare cookies for the requests
cookies = {}
for host, name, value in cookie_data:
    cookies[name] = value

# The target URL
url = 'http://linustechtips.com'

# Make a GET request to the URL with the cookies
response = requests.get(url, cookies=cookies)

# Check if the request was successful
if response.ok:
    print(f"Successfully accessed {url}")
    # The following line saves the content to a file named 'forum_hr.html'
    with open('forum_hr.html', 'w', encoding='utf-8') as file:
        file.write(response.text)
    print("The content has been saved to 'forum_hr.html'.")
else:
    print(f"Failed to access {url}. Status Code: {response.status_code}")
