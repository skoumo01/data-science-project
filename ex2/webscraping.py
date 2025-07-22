from bs4 import BeautifulSoup
import requests
import pandas as pd

# Retrieve webpage
html_data = requests.get('https://web.archive.org/web/20200318083015/https://en.wikipedia.org/wiki/List_of_largest_banks').text

# Parse using BeautifulSoup
soup = BeautifulSoup(html_data, "html.parser")

# Load the data from the "By market capitalization" table into a pandas dataframe
## We need the data from the 3rd table on the page
entries = []
for row in soup.find_all('tbody')[2].find_all('tr'):
    col = row.find_all('td')
    if col != []:
        name = col[1].text
        market_cap = float(col[2].text)
        new_entry_dict = {"Name":name, "Market Cap (US$ Billion)":market_cap}
        # print(new_entry_dict)
        entries.append(new_entry_dict)

# Convert list of dicts to DataFrame
data = pd.DataFrame(entries)

# Display the first 5 rows
data.head()

# Load the pandas dataframe into a JSON called bank_market_cap.json
with open("bank_market_cap.json", "w") as f:
    f.write(data.to_json())

