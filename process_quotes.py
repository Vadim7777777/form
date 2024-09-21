import mysql.connector
import requests
from mysql.connector.cursor import MySQLCursorRaw
from openai import OpenAI
import logging
import threading


sleep_delay = 1;

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

OPENAI_API_KEY = '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'

client = OpenAI(api_key=OPENAI_API_KEY)


def call_api(data):
    # Replace with your actual API endpoint and adjust payload as needed
    api_endpoint = "https://example.com/api"
    payload = {"data": data}
    response = requests.post(api_endpoint, json=payload)

    messages = [{"role": "user", "content":
        "Rewrite all but make sure you replace every therm that relates to croatia to a corresponsing therm as if it would be related to USA. Act as a copywriter and rewrite the resulting text to make it original. Output only the resulting text without any additional comments. Fix any grammatical or spelling errors. If you encounter block in the form \"<NAME> says: <TEXT>\" replace it with \"[QUOTE=name]<text>[/QUOTE]\". [QUOTE] and [/QUOTE] tokens are used to separate a quotation from the rest of the text. It's imperative that you preserve it's location during rewriting, so that they contain only that text that original text belong to. If token \" (double quote character) doesn't have a match, then remove it. Replace tokens of the form (LF) with new lines. Make text style informal, just like it's is on a forum like vbulletin or xenforo. Here is the text: "
                 }]

    try:
        messages[0]["content"] += data
        response = client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=messages
        )
        reply = response.choices[0].message.content
        print(f"ChatGPT: {reply}")
    except Exception as e:
        print(f"Error: {e}")
        return None

    return reply


def update_db(update_conn, update_curr, table_name, row_id, new_data):
    query = f"UPDATE {table_name} SET chatgpt_trans = %s WHERE id = %s"
    update_curr.execute(query, (new_data, row_id))
    update_curr.execute(f"UPDATE {table_name} SET updating = 2 WHERE id = %s", (row_id,))

    logging.info(f"updated row with {row_id} with data {new_data}")


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

            conn = mysql.connector.connect(**db_config)
            update_conn = mysql.connector.connect(**db_config)

            cursor = conn.cursor(buffered=False)
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")

            conn.start_transaction()

            query = f"SELECT id, col1 FROM {table_name} " \
                    f"WHERE chatgpt_trans IS NULL and updating is NULL LIMIT 1 FOR UPDATE"

            cursor.execute(query)

            row = cursor.fetchone()
            if not row:
                break
            row_id, data = row

            logging.info(f"ROW {row_id} selected")

            cursor.execute(f"UPDATE {table_name} SET updating = 1 WHERE id = %s", (row_id,))

            conn.commit()
            cursor.close()
            conn.close()

            api_result = call_api(data)

            update_cursor = update_conn.cursor(buffered=False)
            update_cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            update_conn.start_transaction()

            update_db(update_conn, update_cursor, table_name, row_id, api_result)

            update_conn.commit()
            update_cursor.close()
            update_conn.close()



    except Exception as e:
        print(f"Error: {e}")
        return None


def main():
    # Replace with your actual database connection details

    threads = []
    for _ in range(10):  # Number of threads
        thread = threading.Thread(target=process_row)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
