import pandas as pd
import sqlite3
import requests
import zipfile
import os
import shutil

# Download the dataset and extract the CSV file
url= "https://sdi.eea.europa.eu/datashare/s/ZP4CHfcEpN8jPLs/download"
data_dir = "data"
zip_path = os.path.join(data_dir, "dataset.zip")
csv_path = None
database1 = os.path.join(data_dir,"database1.db")
database2 = os.path.join(data_dir,"database2.db")
database3 = os.path.join(data_dir,"database3.db")
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

response = requests.get(url)
with open(zip_path, 'wb') as file:
    file.write(response.content)

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(data_dir)

for root, dirs, files in os.walk(data_dir):
    for file in files:
        if file.endswith('.csv'):
            csv_path = os.path.join(root, file)
            break
    if csv_path:
        break

if not csv_path:
    raise FileNotFoundError("No CSV file found in the ZIP archive")
shutil.move(csv_path, os.path.join(data_dir, "data1.csv"))


# Download the dataset and extract the CSV file
url2= "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nrg_ind_ren?format=SDMX-CSV&compressed=false"
r = requests.get(url2, allow_redirects=True)
with open('data/data2.csv', 'wb') as file:
    file.write(r.content)

# Download the dataset and extract the CSV file
url3= "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ten00121?format=SDMX-CSV&compressed=false"
r2 = requests.get(url3, allow_redirects=True)
with open('data/data3.csv', 'wb') as file2:
    file2.write(r2.content)

# Read the CSV file and save it to a SQLite database
df1 = pd.read_csv("data/data1.csv")
df2 = pd.read_csv("data/data2.csv")
df3 = pd.read_csv("data/data3.csv")

df1.fillna(0,inplace=True)
df2.fillna(0,inplace=True)
df3.fillna(0,inplace=True)

df1.columns = [ col.strip().lower() for col in df1.columns]
df2.columns = [ col.strip().lower() for col in df2.columns]
df3.columns = [ col.strip().lower() for col in df3.columns]

# Saved file to database
conn1 = sqlite3.connect(database1)
df1.to_sql("dataset1",conn1, if_exists ="replace",index=False)
conn1.close()

conn2 = sqlite3.connect(database2)
df2.to_sql("dataset2",conn2, if_exists ="replace",index=False)
conn2.close()

conn3 = sqlite3.connect(database3)
df3.to_sql("dataset3",conn3, if_exists ="replace",index=False)
conn3.close()

# Remove the downloaded files
shutil.rmtree("data/eea_t_national-emissions-reported_p_2024_v01_r00",)