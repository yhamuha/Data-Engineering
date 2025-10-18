import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
import lxml
import sqlite3

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
output_csv_path = './Largest_banks_data.csv'
exchange_rate_csv_path = './exchange_rate.csv'
sql_connection = sqlite3.connect(db_name)
query1 = "SELECT * FROM Largest_banks;"
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks;"
query3 = "SELECT Name FROM Largest_banks LIMIT 5;"

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

    if not tables:
        print("No tables found.")
        return df

    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) > 2:
            Name = col[1].get_text(strip=True)
            MC_USD_Billion = col[2].get_text(strip=True).replace('\n', '')
            df = pd.concat([df, pd.DataFrame([[Name, MC_USD_Billion]], columns=table_attribs)], ignore_index=True)
            df.index = range(1, len(df) + 1)
    return df

def transform(df, csv_path):

    dataframe = pd.read_csv(exchange_rate_csv_path)
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']

    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_csv_path):
    df.to_csv(output_csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_queries(query_statement, sql_connection):
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)

    rows = cursor.fetchall()

    print("Query Results:")
    for row in rows:
        print(row)

    cursor.close()


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
print(df)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, exchange_rate_csv_path)
print(df)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, output_csv_path)
log_progress('Data saved to CSV file')

load_to_db(df, sql_connection, table_name)
log_progress('SQL Connection initiated')

log_progress('Data loaded to Database as a table, Executing queries')
run_queries(query1, sql_connection)
run_queries(query2, sql_connection)
run_queries(query3, sql_connection)

log_progress('Process Complete')

log_progress('Server Connection closed')