import pandas as pd
import sqlite3
import requests
import zipfile
import os

# link to the dataset
url = "https://sdi.eea.europa.eu/datashare/s/nQm7mfWywfyLdLX/download"

# describing path to save to given directory and file
data_dir = "data"
zip_path = os.path.join(data_dir, "dataset.zip")
csv_path = None
database_path = os.path.join(data_dir, "dataset.db")

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

df = pd.read_csv(csv_path)

# Basic cleaning and transformation of data
df.dropna(inplace=True)  # Drop NA columns
df.columns = [col.strip().lower() for col in df.columns]  # converting names to lower case



# Saving to sqllite3 database
conn = sqlite3.connect(database_path)
df.to_sql("dataset", conn, if_exists="replace", index=False)
conn.close()

print(f"Dataset has been saved to {database_path}")