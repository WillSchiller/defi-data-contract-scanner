import pandas as pd
from multiprocessing import Pool
import psycopg2
import os
from dotenv import load_dotenv
import csv
from io import StringIO
import time

load_dotenv()

# Get timestamp - 7 days
time7DaysAgo = int(time.time()) - 604800

# connection checker 0 = not connected / 1 = connected / if error close connection and set _c to 0 for restart
_c = 0 

#######

# ----------------PostgreSQL connection --------------#
def connect():
    global _c
    if _c == 0:
        try:
            conn = psycopg2.connect(
                host = os.getenv('PSQL_HOST'),
                port = os.getenv('PORT'),
                user = os.getenv('PSQL_USER'),
                password = os.getenv('PASSWORD'),
                database= os.getenv('DB')
                )
            _c = 1
            cursor = conn.cursor()
            return conn, cursor
        except Exception as e:
            print(f"error:connecting: {e}")
            conn.close()
            print("CONNECTION ENDED")
            _c = 0

connection, cursor = connect()

    
# --------------------- HELPERS --------------------- #

top_contracts_tx_count = "CREATE TABLE IF NOT EXISTS top_contracts_tx_count(timestamp INTEGER, blocknumber INTEGER, gas INTEGER, gasPrice Bigint, _from TEXT, contractAddress TEXT, tx_count DECIMAL, eth NUMERIC)"
top_contracts_value = "CREATE TABLE IF NOT EXISTS top_contracts_value(timestamp INTEGER, blocknumber INTEGER, gas INTEGER, gasPrice Bigint, _from TEXT, contractAddress TEXT, tx_count DECIMAL, eth NUMERIC)"

def returnSql(sql):
    cursor.execute(sql)
    result = cursor.fetchall()
    print('SQL executed')
    return result

def executeSql(sql):
    cursor.execute(sql)
    connection.commit()
    print('SQL executed')

def drop_table(table):
    sql = f"DROP TABLE IF EXISTS {table}"
    cursor.execute(sql)
    connection.commit()
    print('dropped')


# SQL join query
sql = F'''
WITH

RAW_DATA AS (
            SELECT 
                *,
                1 AS count
            FROM 
                txdatacontracts 
            ),

TX_COUNT AS (
            SELECT 
                _to, 
                SUM(count) as count, 
                SUM(value) as value
            FROM
                RAW_DATA
            GROUP BY 
                1 
            ),

CONTRACTS AS (
            SELECT 
                timestamp, 
                blocknumber, 
                gas, 
                gasprice,
                _from, 
                contractaddress
            FROM
                RAW_DATA
            WHERE
                contractaddress != ''
            ),   
JOIN_TABLES AS (
            SELECT 
                timestamp, 
                blocknumber, 
                gas, 
                gasprice,
                _from, 
                contracts.contractaddress, 
                TX_COUNT.count as tx_count,
                TX_COUNT.value as eth
            FROM 
                CONTRACTS
            LEFT JOIN
                TX_COUNT
            ON
                CONTRACTS.contractaddress = TX_COUNT._to
            WHERE
                timestamp >= {time7DaysAgo}
            )

SELECT * FROM JOIN_TABLES
''' 

# Save to SQL
def save(df, t):
    try:
        df_ready = df.empty
        if df_ready != True:
            sio = StringIO()
            writer = csv.writer(sio)
            writer.writerows(df.values)
            sio.seek(0)
            with connection.cursor() as c:
                c.copy_from(
                    file=sio,
                    table=t,
                    columns=[
                        "timestamp",
                        "blocknumber",
                        "gas",
                        "gasprice",
                        "_from",
                        "contractaddress",
                        "tx_count",
                        "eth"
                    ],
                    sep=","
                )
                connection.commit()
    except Exception as e:
        print(f"error while saving SQL: {e}")



if __name__ == '__main__':
    data = returnSql(sql)
    df_count = pd.DataFrame(data, columns=['timestamp', 'blocknumber', 'gas', 'gasprice', '_from', 'contractaddress', 'tx_count', 'eth' ]).fillna(0.0).sort_values('tx_count', ascending=False).head(20)
    df_value = pd.DataFrame(data, columns=['timestamp', 'blocknumber', 'gas', 'gasprice', '_from', 'contractaddress', 'tx_count', 'eth' ]).fillna(0.0).sort_values('eth', ascending=False).head(20)
    df_count['eth'] = df_count['eth'].apply(lambda x: '%.5f' % x)
    df_value['eth'] = df_value['eth'].apply(lambda x: '%.5f' % x)
    drop_table('top_contracts_tx_count')
    drop_table('top_contracts_value')
    executeSql(top_contracts_tx_count)
    executeSql(top_contracts_value)
    save(df_count, 'top_contracts_tx_count')
    save(df_value, 'top_contracts_value')


