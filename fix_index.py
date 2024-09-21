import mysql.connector
import requests
from mysql.connector.cursor import MySQLCursorRaw
from openai import OpenAI
import logging
import threading


# Define the function to preserve only the second character from each word in a string
def preserve_second_char_from_each_word(text):
    words = text.split()
    # Keep only the second character from each word if it has at least two letters, otherwise keep the word as is
    modified_words = [word[1] if len(word) > 1 else word for word in words]
    return ' '.join(modified_words)

def process_row():
    try:
        while True:
            db_config = {
                'host': 'localhost',
                'user': 'root',
                'password': '12345',
                'database': 'forumhr',
                'port': 3309
            }
            table_name = 'translation'

            # Connect to the database
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()

            # SQL query to select the texts. Replace 'your_table' and 'your_column' with your actual table and column names
            select_query = "SELECT row_id, col35 FROM all_data_null_id"

            # Execute the select query
            cursor.execute(select_query)

            # Fetch all rows
            rows = cursor.fetchall()

            # Loop through each row, process the text, and update the database
            for row in rows:
                original_text = row[1]  # Assuming the text is in the second column
                modified_text = preserve_second_char_from_each_word(original_text)
                update_query = "UPDATE all_data_null_id SET your_column = %s WHERE id = %s"
                cursor.execute(update_query, (modified_text, row[0]))

            # Commit the changes to the database
            conn.commit()

            # Close the cursor and connection
            cursor.close()
            conn.close()

            print("Database has been updated.")

    except Exception as e:
        print(f"Error: {e}")
        return None


def main():
    # Replace with your actual database connection details
    process_row()

if __name__ == "__main__":
    main()
