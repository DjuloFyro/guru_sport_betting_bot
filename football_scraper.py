import requests
from bs4 import BeautifulSoup
import pandas as pd
from enum import Enum
from time import sleep
import os


class League(Enum):
    """
    This class is used to store the different leagues.
    """
    PREMIER_LEAGUE = "Premier-League"
    LIGUE_1 = "Ligue-1"
    BUNDESLIGA = "Bundesliga"
    SERIE_A = "Serie-A"
    LIGA = "La-Liga"


class FootballScrapper:
    """
    This class is used to scrape football data from the fbref.
    """
    def __init__(self, league_name: League):
        self.league_name = league_name.value
        self.schedule = None
        self.statistics = []


    def get_data(self):
        """
        Scrapes the data from the fbref.
        It scrapes the schedule and the statistics.
        """

        urls = ["https://fbref.com/fr/comps/9/calendrier/Scores-et-tableaux-" + self.league_name,
                "https://fbref.com/fr/comps/9/Statistiques-" + self.league_name]

        for i, url in enumerate(urls):
            # Send an HTTP GET request to the URL
            response = requests.get(url=url)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse the HTML content of the page using BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')

                # Find all the tables on the page
                tables = soup.find_all('table')

                # Initialize a list to store table names
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

                    df = pd.DataFrame(data_rows)

                    # The first url is the schedule
                    if i == 0:
                        self.schedule = df
                    else:
                        self.statistics.append(df)
            else:
                print("Failed to retrieve the webpage. Status code:", response.status_code)
            
            # Wait 1 second to avoid being blocked
            sleep(1)

    def to_csv(self):
        """
        Saves the dataframes to csv files.
        """
        # Create the directory if it does not exist
        if not os.path.exists(f"data/{self.league_name}"):
            os.makedirs(f"data/{self.league_name}")
        
        # Save the schedule
        self.schedule.to_csv(f"data/{self.league_name}/{self.league_name}_schedule.csv")

        # Save the statistics
        for i, df in enumerate(self.statistics):
            df.to_csv(f"data/{self.league_name}/{self.league_name}_stats_{i}.csv")
