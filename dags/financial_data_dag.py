"""
Downloads stock market data from S&P 500 companies and inserts it into CrateDB.

Prerequisites
-------------
In CrateDB, the schema to store this data needs to be created once manually.
See the file setup/financial_data_schema.sql in this repository.

"""
import datetime
import math
import json
import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator


def get_sp500_ticker_symbols():
    """Extracts SP500 companies' tickers from the SP500's wikipedia page"""
    # getting the html code from S&P 500 wikipedia page
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    r_html = requests.get(url,timeout=2.5).text
    soup = BeautifulSoup(r_html, 'html.parser')
    components_table = soup.find_all(id="constituents")

    # the first section (index=0) in the components table contains
    # the headers (which are unimportant in this context) therefore,
    # only data from index=1 on is taken. moreover, each row in the
    # table is stored in a <tr> division, so we find all of those.
    # The data is stored in an array, where each element contains
    # information about a S&P 500 company.
    data_rows = components_table[0].find("tbody").find_all("tr")[1:]
    tickers = []

    # extracting the tickers from the data
    for row in range(len(data_rows)):
        stock = list(filter(None,data_rows[row].text.split("\n")))
        symbol = stock[0]

        if symbol.find('.') != -1:
            symbol = symbol.replace('.', '-')

        tickers.append(symbol)

    tickers.sort()

    return tickers

def download_yfinance_data_function(start_date):
    """downloads Adjusted Close data from SP500 companies"""

    tickers = get_sp500_ticker_symbols()
    data = yf.download(tickers, start=start_date)['Adj Close']
    return data.to_json()

def prepare_data_function(ti):
    """creates an array of formatted and clean data values"""

    # pulling data (as string)
    string_data = ti.xcom_pull(task_ids='download_data_task')

    # transforming to json
    json_data = json.loads(string_data)

    # transforming to dataframe for easier manipulation
    df = pd.DataFrame.from_dict(json_data, orient='index')

    values_array = []

    for col in range(len(df.columns)):
        closing_date = df.columns[col]

        for row in range(len(df.index)):
            ticker = df.index[row]
            adj_close = df.iloc[row, col]

            if not(adj_close is None or math.isnan(adj_close)):
                values_array.append(f"({closing_date}, '{ticker}', {adj_close}")
                #values_array.append("({},\'{}\',{})".format(closing_date, ticker, adj_close))

    return values_array

def insert_data_function(ti):
    """inserts financial data values into CrateDB"""

    values_array = ti.xcom_pull(task_ids='prepare_data_task')

    insert_stmt = "INSERT INTO sp500 (closing_date, ticker, adjusted_close) VALUES "
    insert_stmt += ", ".join(values_array) + ";"

    insert_data_task = PostgresOperator(
                task_id="insert_data_task",
                postgres_conn_id="cratedb_connection",
                sql=insert_stmt
                )

    insert_data_task.execute(dict())


with DAG(
    dag_id="financial_data_dag",
    start_date=datetime.datetime(2022, 1, 10),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    download_data_task = PythonOperator(task_id='download_data_task',
                                    python_callable=download_yfinance_data_function,
                                    provide_context=True,
                                    op_kwargs={
                                        "start_date": "{{ ds }}",
                                    },
                                    execution_timeout=datetime.timedelta(minutes=3))

    prepare_data_task = PythonOperator(task_id='prepare_data_task',
                                    python_callable=prepare_data_function,
                                    provide_context=True,
                                    op_kwargs={},
                                    execution_timeout=datetime.timedelta(minutes=3))

    insert_data_task = PythonOperator(task_id='insert_data_task',
                                    python_callable=insert_data_function,
                                    provide_context=True,
                                    op_kwargs={},
                                    execution_timeout=datetime.timedelta(minutes=3))

download_data_task >> prepare_data_task >> insert_data_task
