url="https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
csv_path = './Countries_by_GDP.csv'
table_name = 'Countries_by_GDP'
db_name = 'World_Economies.db'
table_attributes=['Country','GDP_USD_billion']
logs_file='./etl_project_log.txt'

# Importing the required libraries
import requests  # HTTP Protocol (1.1)
from bs4 import BeautifulSoup # Used for Web Scarping
import pandas as pd  # Processing extracted data
import sqlite3  # Dummy Database
import numpy as np  # Mathematical rounding
from datetime import datetime  # Timestamp

# Extract
def extract(url,table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html_page = requests.get(url).text
    # Parse Html page
    soup = BeautifulSoup(html_page,'html.parser')
    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')
    # DataFrame for appending the data:
    df= pd.DataFrame(columns= table_attributes)
    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0: # Row should not be be empty
            if cols[0].find('a') is not None and '—' not in cols[2]:
                data_dict = {
                    'Country': cols[0].a.contents[0],
                    'GDP_USD_billion': cols[2].contents[0]
                }
                df1 = pd.DataFrame(data_dict,index=[0])
                df = pd.concat([df,df1],ignore_index=True)
    return df

# Transform
def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 holadecimal places.
    The function returns the transformed dataframe.'''
    GDP_list = df['GDP_USD_billion'].tolist()
    GDP_list =[float(''.join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df['GDP_USD_billion'] = GDP_list
    df = df.rename(columns={'GDP_USD_million':'GDP_USD_billion'})
    return df

# Load
def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

# DB Connection
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name,sql_connection,if_exists='replace',index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement,sql_connection)
    print(query_output)


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(logs_file,'a') as f:
        f.write(timestamp + ', ' + message + '\n')
''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process.')

df = extract(url,table_attributes)

log_progress('Data extraction complete. Initiating Transformation process.')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process.')

load_to_csv(df,csv_path)

log_progress('Data saved to CSV file.')

conn = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df,conn,table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billion >= 100"
run_query(query_statement, conn)

log_progress('Process Complete.')

conn.close()