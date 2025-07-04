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
import logging
import pendulum
import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task


def get_sp500_ticker_symbols():
    """Extracts S&P 500 companies' tickers from the S&P 500's wikipedia page"""

    # Getting the html code from S&P 500 wikipedia page
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    r_html = requests.get(url, timeout=2.5).text
    soup = BeautifulSoup(r_html, "html.parser")

    # The stock tickers are found in a table in the wikipedia page,
    # whose html "id" attribute is "constituents". Here, the html
    # soup is filtered to get the  table contents
    table_content = soup.find(id="constituents")

    # Each stock's information is stored in a 'tr' division,
    # so we use this as a filter to generate a list of stock data.
    # The first section (index=0) in the generated list contains
    # the headers (which are unimportant in this context), therefore,
    # only data from index=1 on is taken.
    # For mapping, we find the ticker in the first 'td' division of
    # each stock and replace, when given, a '.' (wikipedia notation)
    # with a '-' (yfinance notation).
    # Finally, the map is returned as a list.
    return list(
        map(
            lambda stock: stock.find("td").text.strip().replace(".", "-"),
            table_content.find_all("tr")[1:],
        )
    )


@task(execution_timeout=datetime.timedelta(minutes=3))
def download_yfinance_data(ds=None):
    """Downloads Close data from S&P 500 companies"""

    tickers = get_sp500_ticker_symbols()
    data = yf.download(tickers, start=ds)["Close"]
    return data.to_json()


@task(execution_timeout=datetime.timedelta(minutes=3))
def prepare_data(string_data):
    """Creates a list of dictionaries with clean data values"""

    # transforming to dataframe for easier manipulation
    df = pd.DataFrame.from_dict(json.loads(string_data), orient="index")

    values_dict = []
    for col, closing_date in enumerate(df.columns):
        for row, ticker in enumerate(df.index):
            adj_close = df.iloc[row, col]

            if not (adj_close is None or math.isnan(adj_close)):
                values_dict.append(
                    {
                        "closing_date": closing_date,
                        "ticker": ticker,
                        "adj_close": adj_close,
                    }
                )
            else:
                logging.info(
                    "Skipping %s for %s, invalid adj_close (%s)",
                    ticker,
                    closing_date,
                    adj_close,
                )

    return values_dict


@dag(
    start_date=pendulum.datetime(2022, 1, 10, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def financial_data_import():
    yfinance_data = download_yfinance_data()

    prepared_data = prepare_data(yfinance_data)

    SQLExecuteQueryOperator.partial(
        task_id="insert_data_task",
        conn_id="cratedb_connection",
        sql="""
            INSERT INTO doc.sp500 (closing_date, ticker, adjusted_close)
            VALUES (%(closing_date)s, %(ticker)s, %(adj_close)s)
            ON CONFLICT (closing_date, ticker) DO UPDATE SET adjusted_close = excluded.adjusted_close
            """,
    ).expand(parameters=prepared_data)


financial_data_import()
