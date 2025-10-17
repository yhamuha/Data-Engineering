import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy
from datetime import datetime
import lxml

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt","a") as f:
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url)
    data = BeautifulSoup(page.text, 'lxml')

    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')

    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')

        if len(col) > 2:
            Name = col[1].get_text(strip=True)
            MC_USD_Billion = col[2].get_text(strip=True).replace('\n', '')
            print(Name, MC_USD_Billion)
    return df

def transform(df, csv_path):

    return df

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')