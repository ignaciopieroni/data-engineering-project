# Known information
url='https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attributes_extraction=['Name','MC_USD_Billion']
table_attributes_final = ['Name','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']
csv_path = './Largest_banks_data.csv'
db_name= 'Banks.db'
table_name='Largest_banks'
logs_file='code_log.txt'


# Importing the required libraies
from bs4 import BeautifulSoup # Used for Web Scraping
import requests # HTTP Protocol (1.1)
import pandas as pd # Processing extracted data
import numpy as np # Arithmetic/Algebra/Mathematical Rounding
from datetime import datetime # Timestamp
import sqlite3 # Dummy Data Base




# Code for ETL operations on Country-GDP data


# Log:
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(logs_file,'a') as f:
        f.write(timestamp+', '+message+'\n')


# Extract:
def extract(url,table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    html_page = requests.get(url).text

    # Parse HTML page
    soup = BeautifulSoup(html_page,'html.parser')
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')

    # DataFrame for appending data
    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        cols = row.find_all('td') # Extract cells <td>
        for col in cols: # Iterate through each <td>
            for child in col.children: #Access the children of each <td>
                if child.name == 'a': #Filter out the text nodes and staying with hyperlinks (name of banks)

                    name=child.text
                    market_cap = float(cols[2].text.strip()) # Strip for removing the last caracter of market_cap ('\n')

                    data_dict={
                        'Name' : name,
                        'MC_USD_Billion' : market_cap 
                    }

                    df1 = pd.DataFrame(data_dict,index = [0])
                    df = pd.concat([df,df1],ignore_index=True)
    return df


# Transform:
def transform(df,csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''


    # Download the provided CSV File (exchange information)
    response = requests.get(csv_path)
    if response.status_code == 200:
        with open('exchange_rate.csv','wb') as f:
            f.write(response.content)


    # Now let's read the exchange rate CSV file and convert the contents to a dictionary so that the contents of the first columns are the keys to the dictionary and the contents of the second column are the corresponding values.
    exchange_df = pd.read_csv(csv_path)

    contents_dictionary = exchange_df.set_index(exchange_df.columns[0]).to_dict()[exchange_df.columns[1]]
    
    # Finally let's add 3 different columns to the dataframe, viz. MC_GBP_Billion, MC_EUR_Billion and MC_INR_Billion, each containing the content of MC_USD_Billion scaled by the corresponding exchange rate factor. Remember to round the resulting data to 2 decimal places.
    df['MC_GBP_Billion'] = np.round([x*contents_dictionary['GBP'] for x in df['MC_USD_Billion']],2)
    df['MC_EUR_Billion'] = np.round([x*contents_dictionary['EUR'] for x in df['MC_USD_Billion']],2)
    df['MC_INR_Billion'] = np.round([x*contents_dictionary['INR'] for x in df['MC_USD_Billion']],2)

    return df

# Load:
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name,sql_connection,if_exists='replace',index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement,sql_connection)
    print(query_output)


''' Defining the required entities and call the relevant
functions in the correct order to complete the project'''

log_progress('Preliminaries complete. Initiating ETL process.')

df = extract(url,table_attributes_extraction)

log_progress('Data extraction complete. Initiating Transformation process.')

df = transform(df,exchange_rate_csv)

log_progress('Data transformation complete. Initiating loading process.')

load_to_csv(df,csv_path)

log_progress('Data saved to CSV file.')

# DB Connection
conn = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df,conn,table_name)

log_progress('Data loaded to Database as table. Executing queries')

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, conn)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, conn)

query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, conn)

log_progress('Process Complete.')

conn.close()

log_progress('Server Connection closed')