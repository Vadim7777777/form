import validators
from lxml import html
import requests
import math

def filter_ascii(s):
    return ''.join(c for c in s if ord(c) < 128)


def string_to_ascii_string(s):
    ascii_string = ''.join(str(ord(c)) for c in s)
    return ascii_string


def main():
    # Replace with your actual database connection details
    # Using readlines()
    file1 = open('../links_forumhr_threads.txt', 'r')
    Lines = file1.readlines()

    dict = {}

    count = 0
    # Strips the newline character

    max_page_number = 0

    with open('../links_forumhr_threads.txt', 'r') as file:
        for line in file:
            parts = line.split('&')
            parts[0] = parts[0].strip()

            if parts[0] not in dict.keys():
                dict[parts[0]] = 1

            for part in parts:
                keyValue = part.split('=')
                if 'page' == keyValue[0]:
                    if parts[0] in dict.keys():
                        dict[parts[0]] = max(int(dict[parts[0]]), int(keyValue[1].strip()))
                    else:
                        dict[parts[0]] = int(keyValue[1].strip())

    file2 = open("links_forumhr_threads_filledinwithpages.txt", "w")

    for entry in dict:

        k = dict[entry]

        while len(entry) > 0 and entry[0] != 'h':
            entry = entry[1:]

        if len(entry) > 0:
            url = entry.strip()

            #url = filter_ascii(url)

            file2.write(url + "\n")
            validation = validators.url(entry.strip())
            if not validation:
                print("URL is invalid")

            for i in range(2, int(k) + 1):
                url = (entry.strip() + "&page=" + str(i)).strip()
                #url = filter_ascii(url)

                file2.write(url + "\n")

                validation = validators.url(entry.strip())
                if not validation:
                    print("URL is invalid")

    file2.close()


if __name__ == "__main__":
    main()
