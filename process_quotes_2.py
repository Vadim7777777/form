from time import sleep

import logging
import mysql.connector
import hashlib
from mysql.connector import errors
import requests
from mysql.connector.cursor import MySQLCursorRaw
from openai import OpenAI
import logging
import threading

import re


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

            logging.disable(100)

            conn = mysql.connector.connect(**db_config)

            cursor = conn.cursor(buffered=False)
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")

            while True:

                # conn.start_transaction()

                print("Query db start")

                query = f"SELECT id, col12 FROM {table_name} " \
                        f"WHERE quote_extracted is null limit 10000"

                print("Query db end")

                cursor.execute(query)

                rows = cursor.fetchall()

                stack = []

                if len(rows) == 0:
                    break

                rowNum = 0

                for row in rows:
                    row_id, col12 = row

                    if row_id == 5230822 or row_id == 5360585 or row_id == 10496962:#undo log too big
                        continue

                    col122 = col12

                    print(f"ROW {row_id} selected")

                    openingQuote = '[QUOTE'
                    closingQuote = '[/QUOTE]'

                    col12Upper = col12.upper()

                    index = 0

                    while index < len(col122):
                        if col12Upper[index:index + len(openingQuote)] == openingQuote:
                            stack.append(index)

                            restOfStringEndSearchIndex = index + len(openingQuote)

                            if restOfStringEndSearchIndex < len(col122) \
                                    and not (closingQuote in col12Upper[restOfStringEndSearchIndex:]) \
                                    and not (openingQuote in col12Upper[restOfStringEndSearchIndex:]):

                                col12Upper += "[/QUOTE]"
                                col12 += "[/quote]"

                        elif col12Upper[index:index + len(closingQuote)] == closingQuote:
                            # found quote ending

                            try:
                                openingQuoteIndex = stack.pop()
                            except Exception as e:
                                index += 1
                                continue
                            quoteText = col12[openingQuoteIndex:(index + len(closingQuote))]
                            quoteTextTruncated = quoteText.lower()
                            quoteTextTruncated = ''.join(char for char in quoteTextTruncated if char.isalnum())
                            quoteTextTruncated = quoteTextTruncated.encode('ascii', 'replace').decode('ascii')

                            hash_object = hashlib.sha512()

                            # Update the hash object with the bytes of the input string
                            hash_object.update(quoteTextTruncated.encode())

                            # Obtain the hexadecimal digest (the hash value as a hexadecimal string)
                            quoteTextTruncatedHash = hash_object.hexdigest()

                            insert_query = "insert into forumhr.quotes (quote, quotestripped) values (%s, %s)"

                            try:
                                cursor.execute(insert_query, (quoteText, quoteTextTruncatedHash))
                                conn.commit()

                                lastRowId = cursor.lastrowid

                            except mysql.connector.Error as err:
                                if err.errno == mysql.connector.errorcode.ER_DUP_ENTRY:

                                    find_id_query = "select id from forumhr.quotes where quotestripped = (%s)"
                                    cursor.execute(find_id_query, (quoteTextTruncatedHash,))
                                    record = cursor.fetchone()
                                    if record is None:
                                        # query = f"update {table_name} " \
                                        #         f"set quote_extracted = 4 " \
                                        #         f"WHERE id = %s"
                                        # cursor.execute(query, (row_id,))
                                        print(f"Empty record in duplicate lookup query!")
                                    lastRowId = record[0]

                                    print(f"Error: Duplicate entry for a key. Found row with id {lastRowId}")
                                else:
                                    # Handle other errors
                                    print(f"Error: {err}")

                            id_token = f"QUOTE_ID_{lastRowId}"
                            col12 = col12.replace(quoteText, id_token)
                            col12Upper = col12Upper.replace(quoteText.upper(), id_token)
                            index = index + len(closingQuote) - len(quoteText) + len(id_token) - 1

                        index += 1

                    if col122 != col12:
                        try:
                            update_col12_row_token = "update translation set col11 = %s where id = %s"
                            values = (col12, row_id)
                            cursor.execute(update_col12_row_token, values)

                            query = f"update {table_name} " \
                                    f"set quote_extracted = 1 " \
                                    f"WHERE id = %s"
                            cursor.execute(query, (row_id,))
                            conn.commit()
                        except Exception as e:
                            query = f"update {table_name} " \
                                    f"set quote_extracted = 20 " \
                                    f"WHERE id = %s"
                            cursor.execute(query, (row_id,))
                    else:
                        query = f"update {table_name} " \
                                f"set quote_extracted = 2 " \
                                f"WHERE id = %s"
                        try:
                            cursor.execute(query, (row_id,))
                        except mysql.connector.Error as e:
                            if e.errno == 1713:
                                continue

                # conn.commit()
                cnt = {len(rows)}
                print(f"{cnt} rows commited.")

    except Exception as e:
        print(f"Error: {e}")
        return None


def main():
    # Replace with your actual database connection details

    # threads = []
    # for _ in range(1):  # Number of threads
    #     thread = threading.Thread(target=process_row)
    #     threads.append(thread)
    #     thread.start()
    #
    # for thread in threads:
    #     thread.join()
    process_row()


if __name__ == "__main__":
    main()
