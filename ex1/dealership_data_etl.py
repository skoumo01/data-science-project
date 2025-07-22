from datetime import datetime
import pandas as pd                 # this module helps in processing CSV files
tmpfile    = "dealership_temp.tmp"               # file used to store all extracted data
logfile    = "dealership_logfile.txt"            # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"   # file where transformed data is stored

import glob                         # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.

# CSV extract function
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# JSON extract function
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

# XML extract function
def extract_from_xml(file_to_process):
    records = []
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = int(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        records.append({
            "car_model": car_model,
            "year_of_manufacture": year_of_manufacture,
            "price": price,
            "fuel": fuel
        })
    return pd.DataFrame(records)

# General extract function
def extract():
    extracted_data = []  # list to collect all dataframes

    # Process all CSV files
    for csvfile in glob.glob("dealership_data/*.csv"):
        extracted_data.append(extract_from_csv(csvfile))

    # Process all JSON files
    for jsonfile in glob.glob("dealership_data/*.json"):
        extracted_data.append(extract_from_json(jsonfile))

    # Process all XML files
    for xmlfile in glob.glob("dealership_data/*.xml"):
        extracted_data.append(extract_from_xml(xmlfile))

    # Concatenate all DataFrames
    return pd.concat(extracted_data, ignore_index=True)


# Add the transform function below
def transform(data):
        data['price'] = round(data.price,2)
        return data

# Add the load function below
def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile) 


# Log that you have started the ETL process
print("ETL Job Started")

# Log that you have started the Extract step
print("Extract phase Started")

# Call the Extract function
extracted_data = extract()

# Log that you have completed the Extract step
print("Extract phase Ended")


# Log that you have started the Transform step
print("Transform phase Started")

# Call the Transform function
transformed_data = transform(extracted_data)

# Log that you have completed the Transform step
print("Transform phase Ended")


# Log that you have started the Load step
print("Load phase Started")

# Call the Load function
load(targetfile, transformed_data)

# Log that you have completed the Load step
print("Load phase Ended")

# Log that you have completed the ETL process
print("ETL Job Ended")



