# Objectives:
# Create a database using Python
# Load the data from a CSV file as a table to the database
# Run basic "queries" on the database to access the information

# Scenario
# Consider a dataset of employee records that is available with an HR team in a CSV file. Create the database called STAFF and load the contents of the CSV file as a table called INSTRUCTORS. The headers of the available data are :

# Header	Description
# ID	Employee ID
# FNAME	First Name
# LNAME	Last Name
# CITY	City of residence
# CCODE	Country code (2 letters)


# For the purpose of this proyect, we are going to create the database on a dummy server using SQLite3 library.
# Download the required data


import requests

url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/INSTRUCTOR.csv"

response = requests.get(url)

if response.status_code == 200:
    with open('INSTRUCTOR.csv','wb') as f:
        f.write(response.content)

# Import the required libraries

import sqlite3
import pandas as pd

# Connect to the sqlite3 service
conn = sqlite3.connect('STAFF.db')

# Define table parametters
table_name = 'INSTRUCTOR'
attribute_list = ['ID','FNAME','LNAME','CITY','CCODE']

# Read the CSV data
file_path = 'C:/Users/si/Desktop/github/python_project_for_data_engineering/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

# Load CSV file to the database
df.to_sql(table_name,conn,if_exists='replace', index=False)
print('Table is ready')

# Query 1: Display all rows of the table
query_statement= f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)

# Query 2: Display the number of rows of the table
query_statement = f'SELECT COUNT(*) FROM {table_name}'
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)

# Query 3: Dislpay only the FNAME column for the full table
query_statement = f'SELECT FNAME FROM {table_name}'
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)

# Defining data to be appended
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}

data_append = pd.DataFrame(data_dict)

# Append data to the table
data_append.to_sql(table_name,conn,if_exists = 'append',index = False)
print('Data appended succesfully')


# Query 4: Display the count of the total number of rows
query_statement = f'SELECT COUNT(*) FROM {table_name}'
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)




# ====================================================================================================================================





# Now. In the same database STAFF, we're going to create another table called Departments. The attributes of the table are as shown below.

# Header	Description
# DEPT_ID	Department ID
# DEP_NAME	Department Name
# MANAGER_ID	Manager ID
# LOC_ID	Location ID

new_table_name='DEPARTMENTS'
new_attributes_list=['DEPT_ID','DEP_NAME','MANAGER_ID','LOC_ID']

new_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/Departments.csv"

response = requests.get(new_url)

if response.status_code == 200:
    with open('DEPARTMENTS.csv','wb') as f:
        f.write(response.content)

new_file_path='C:/Users/si/Desktop/github/python_project_for_data_engineering/DEPARTMENTS.csv'

new_df = pd.read_csv(new_file_path,names=new_attributes_list)


# Load the new table to the data base 
new_df.to_sql(new_table_name,conn,if_exists='replace',index=False)

# Query 5: Display added table to the data base
query_statement = f'SELECT * FROM {new_table_name}'
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)


# Query 6: Display the count of the total number of rows
query_statement = f'SELECT COUNT(*) FROM {new_table_name}'
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)

# Query 7: Retrieve the data within the department name attribute of the table
query_statement = f'SELECT DEP_NAME FROM {new_table_name}'
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)


# Finally let's append the following information to the Departments' table:
new_data_dict = {
    'DEPT_ID' : ['9'],
    'DEP_NAME' : ['Quality Assurance'],
    'MANAGER_ID' : ['30010'],
    'LOC_ID' : ['L0010']
}
new_data_append = pd.DataFrame(new_data_dict)

# Appending data to the Departments' table
new_data_append.to_sql(new_table_name,conn,if_exists='append',index=False)
print('New data appended succesfully')


# And let's check the changes!

# Query 8: Display the count of the total number of rows for the full new table
query_statement = f'SELECT COUNT(*) FROM {new_table_name}'
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)

# Query 9: Display the full table
query_statement = f'SELECT * FROM {new_table_name}'
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)

# Query 10: See all the tables in our data base.
query_statement = "SELECT COUNT(*) FROM sqlite_master WHERE type='table';"
cursor = conn.cursor()
cursor.execute(query_statement)

# Fetch and print the result
table_count = cursor.fetchone()[0]
print(f"Number of tables: {table_count}")

# Close the connection
conn.close()