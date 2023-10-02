import requests
from bs4 import BeautifulSoup
import pandas as pd

# URL of the webpage to scrape
#url = "https://fbref.com/fr/comps/9/Statistiques-Premier-League"

url2 = "https://fbref.com/fr/comps/9/calendrier/Scores-et-tableaux-Premier-League"

# Send an HTTP GET request to the URL
response = requests.get(url2)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the HTML content of the page using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all the tables on the page
    tables = soup.find_all('table')

    # Initialize a list to store DataFrames
    dataframes = []

    table_names = []

    # Loop through each table and extract the data
    for table in tables:
        # Process and extract data from the table as needed
        data_rows = []

         # Extract the table caption (if available) as the table name
        caption = table.find('caption')
        table_name = caption.text.strip() if caption else None
        table_names.append(table_name)

        # Extract table data rows
        rows = table.find_all('tr')
        for row in rows:
            data_row = []
            td_elements = row.find_all(['td', 'th'])  # Include 'th' elements for header rows
            for td in td_elements:
                # Check for colspan attribute
                colspan = int(td.get('colspan', 1))
                cell_text = td.text.strip()
                # Replicate cell data for multiple columns if colspan > 1
                for _ in range(colspan):
                    data_row.append(cell_text)
            data_rows.append(data_row)

        # Create a Pandas DataFrame
        df = pd.DataFrame(data_rows)

        # Append the DataFrame to the list
        dataframes.append(df)

        

    # Now you have a list of DataFrames, one for each table on the page
    # You can access and manipulate these DataFrames as needed

    # For example, to access the first DataFrame:
    for i, df in enumerate(dataframes):
        if i % 2 == 0:
            df.to_csv(f"data/schedule/{table_names[i]}_{i}.csv")
        else:
            df.to_csv(f"data/{table_names[i]}.csv")

else:
    print("Failed to retrieve the webpage. Status code:", response.status_code)
