import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3

# TOP 50 Films

# We are required to write a Python script that extracts the information and saves it to a CSV file top_50_films.csv. Also save the same information to a database Movies.db under the table name Top_50.
# The information required is Average Rank, Film, Year and Rotten Tomatoes' Top 100.

url="https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"
csv_path="./top_50_films.csv"
db_name="Movies.db"
table_name="Top_50"
df = pd.DataFrame(columns=['Average Rank','Film','Year'])
count = 0

# Loading the webpage for WebScraping

html_page = requests.get(url).text
soup = BeautifulSoup(html_page,'html.parser')

# Analazing the HTML Code for relevant information

tables = soup.find_all('tbody')
rows = tables[0].find_all('tr')

for i in rows:
    if count < 50:
        j = i.find_all('td')
        if len(j) != 0:
            data_dictionary={
                'Average Rank' : int(j[0].contents[0]), #.'contents[0]' is for extracting the first child node.
                'Film' : j[1].contents[0],
                'Year' : int(j[2].contents[0]),
                "Rotten Tomatoes' Top 100" : j[3].contents[0]
            }
            df1 = pd.DataFrame(data_dictionary,index=[0])
            df = pd.concat([df,df1],ignore_index=True)
            count+=1
    else: 
        break

# Loading .csv file
df.to_csv(csv_path)



# To store the required data in a database, we first need to initialize a connection to the database, save the dataframe as a table, and then close the connection.
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()


# Films released in the 2000s (year 2000 included).

print(df.loc[df['Year'] >= 2000])
