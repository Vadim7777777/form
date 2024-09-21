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

        file1 = open('../computed_links.txt', 'a')
        file1.writelines(line);

        minLeft = 0
        maxRight = 510

        while minLeft != maxRight:

            lineN = line + f"&page={math.trunc((minLeft + maxRight) / 2)}&sort=views&order=desc&daysprune=-1"

            page = requests.get(lineN)

            # Parsing the page
            # (We need to use page.content rather than
            # page.text because html.fromstring implicitly
            # expects bytes as input.)
            tree = html.fromstring(page.content)

            # Get element using XPath
            buyers = tree.xpath("//a[contains(@rel, 'next')]")
            if buyers:
                minLeft = math.trunc((minLeft + maxRight + 1) / 2)
            else:
                maxRight = math.trunc((minLeft + maxRight) / 2)

        print(f"min left: {minLeft}")
        print(f"max left: {maxRight}")

        for x in range(1, math.trunc((minLeft + maxRight) / 2) + 1):
            if x == 1:
                file1.writelines(line.strip() + "\n")
            else:
                file1.writelines(line.strip() + f"&page={x}&order=desc&sort=views&order=desc&daysprune=-1\n")

        file1.close()


if __name__ == "__main__":
    main()
