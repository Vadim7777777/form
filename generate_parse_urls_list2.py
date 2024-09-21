from lxml import html
import requests
import math

def main():
    # Replace with your actual database connection details
    # Using readlines()
    file1 = open('../forums_with_counts.txt', 'r')
    Lines = file1.readlines()

    count = 0
    # Strips the newline character

    for line in Lines:
        count += 1

        counter = 2
        maxCounter = 500

        file1 = open('../computed_links2.txt', 'a')

        dict = line.split(",")

        file1.writelines(dict[0].strip() + "\n")

        if dict[1].strip().isnumeric():
            for i in range(1, int(dict[1].strip())):

                lineN = dict[0] + f"&page={i}&sort=views&order=desc&daysprune=-1"

                file1.writelines(lineN.strip() + "\n")
        file1.close()


if __name__ == "__main__":
    main()
