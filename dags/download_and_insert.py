import datetime
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd
import math
import json

def get_sp500_ticker_symbols():
    
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    r = requests.get(url,timeout=2.5)
    r_html = r.text
    soup = BeautifulSoup(r_html, 'html.parser')
    
    components_table= soup.find_all(id = "constituents")
    data_rows = components_table[0].find("tbody").find_all("tr")[1:]

    tickers = []
    
    for row in range(len(data_rows)):
        stock = list(filter(None,data_rows[row].text.split("\n")))
        symbol = stock[0]
        
        if (symbol.find('.') != -1):
            symbol = symbol.replace('.', '-')
            
        tickers.append(symbol)
        
    tickers.sort()
    
    return tickers

def download_YFinance_data_function():

    tickers = get_sp500_ticker_symbols()
    data = yf.download(tickers[:5], start = '2021-11-30')['Adj Close']
    return data.to_json()

def format_insert_function(**kwargs):
    
    ti = kwargs['ti']
    
    # pulling data (as string)
    string_data = ti.xcom_pull(task_ids = 'download_data_task')
    
    # transforming to json
    json_data = json.loads(string_data)
    
    # transforming to dataframe for easier manipulation
    df = pd.DataFrame.from_dict(json_data, orient = 'index')

    insert_stmt = "INSERT INTO sp500 (closing_date, ticker, adjusted_close) VALUES "

    for col in range(len(df.columns)):
        values_array = []
        closing_date = df.columns[col]
        
        for row in range(len(df.index)):
            ticker = df.index[row]
            adj_close = df.iloc[row, col]
            
            if (not(adj_close is None or math.isnan(adj_close))):
                values_array.append("({},\'{}\',{})".format(closing_date, ticker, adj_close))

        insert_stmt += ", ".join(values_array) + ","
        
    insert_stmt = insert_stmt[:len(insert_stmt)-1] + ';'
    print(insert_stmt)
    
    return  insert_stmt

def insert_data_function(**kwargs):
    
    ti = kwargs['ti']
    insert_stmt = ti.xcom_pull(task_ids = 'format_insert_task')

    insert_data_task = PostgresOperator(
                task_id="insert_data_task",
                postgres_conn_id="cratedb_connection",
                sql=insert_stmt
                )
    
    insert_data_task.execute(dict())
    

with DAG(
    dag_id="financial_data_dag",
    start_date=datetime.datetime(2021, 11, 11),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    create_table_task = PostgresOperator(
                task_id="create_table_task",
                postgres_conn_id="cratedb_connection",
                sql="""
                        CREATE TABLE IF NOT EXISTS sp500 (closing_date TIMESTAMP, ticker TEXT, adjusted_close FLOAT)
                    """
                )
   
    download_data_task = PythonOperator(task_id = 'download_data_task',
                                        python_callable = download_YFinance_data_function,
                                        provide_context=True,
                                        execution_timeout=datetime.timedelta(minutes=3))

    format_insert_task = PythonOperator(task_id = 'format_insert_task',
                                      python_callable = format_insert_function,
                                      provide_context=True,
                                      execution_timeout=datetime.timedelta(minutes=3))
    insert_data_task = PythonOperator(task_id = 'insert_data_task',
                                      python_callable = insert_data_function,
                                      provide_context=True,
                                      execution_timeout=datetime.timedelta(minutes=3))

create_table_task >> download_data_task >> format_insert_task >> insert_data_task
