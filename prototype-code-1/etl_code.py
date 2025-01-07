# Firstly we download the zip file containing the required data in multiple formats

import requests

url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip"

response = requests.get(url)

if response.status_code == 200:
    with open('source.zip','wb') as f:
        f.write(response.content)

# Unzip file (console command) -> tar -xf source.zip

# Output files:

logs_file='logs_process.txt'
target_file='transformed_data.csv'


# Extract:

# importing the required libraries for extracting data
import pandas as pd
import numpy as np
import xml.etree.ElementTree as etree
import glob

from datetime import datetime


def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df

def extract_from_json(file_to_process):
    # lines=True is so that the function reads the file as a JSON object on line to line basis
    df = pd.read_json(file_to_process,lines=True)
    return df

def extract_from_xml(file_to_process):
    df = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])

    #Parse the .xml file
    tree=etree.parse(file_to_process)
    root=tree.getroot()
    for car in root:

        car_model=car.find('car_model').text
        year_of_manufacture=float(car.find('year_of_manufacture').text) # Cast the data type to float
        price=float(car.find('price').text) # Cast the data type to float
        fuel=car.find('fuel').text

        new_row=pd.DataFrame([{'car_model':car_model,'year_of_manufacture':year_of_manufacture,'price':price,'fuel':fuel}])

        df=pd.concat([df,new_row],ignore_index=True)

    return df



def extract():

    # create new DataFrame to hold extracted data:
    extracted_data = pd.DataFrame(columns = ['car_model','year_of_manufacture','price','fuel'])

    # extract data from .csv files
    for csv_file in glob.glob('*.csv'):
        extracted_data = pd.concat([extracted_data,pd.DataFrame(extract_from_csv(csv_file))],ignore_index=True)
    
    # extract data from .json files
    for json_file in glob.glob('*.json'):
        extracted_data = pd.concat([extracted_data,pd.DataFrame(extract_from_json(json_file))],ignore_index=True)
    
    # extract data from .xml files
    for xml_file in glob.glob('*.xml'):
        extracted_data = pd.concat([extracted_data,pd.DataFrame(extract_from_xml(xml_file))],ignore_index=True)

    return extracted_data


# Transform:

# Transform the values under the 'price' header so that they are rounded to 2 decimal places
def transform(data):
    data['price'] = round(data.price,2)
    return data


# Load:
def load_data(target_file,transformed_data):
    transformed_data.to_csv(target_file)

# Logs:
def log_progress(message):
    timestamp_format='%Y-%h-%d-%H:%M:%S'
    now = datetime.now() #Get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(logs_file,'a') as f:
        f.write(timestamp+', '+message+'\n')

# See the tests ('testing_etl_ops.py')