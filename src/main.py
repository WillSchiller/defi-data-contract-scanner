from datetime import datetime
from web3 import Web3
import pandas as pd
from multiprocessing import Pool
import psycopg2
import os
from dotenv import load_dotenv
import csv
from io import StringIO

load_dotenv()

w3 = Web3(Web3.HTTPProvider(os.getenv('RPC'))) # Web3 endpoint
ts = 0 # timestamp
_c = 0 # connection checker 0 = not connected / 1 = connected / if error close connection and set _c to 0 for restart



# -------------- PostgreSQL connections ------------- #
def connect():
    conn = psycopg2.connect(
        host = os.getenv('PSQL_HOST'),
        port = os.getenv('PORT'),
        user = os.getenv('PSQL_USER'),
        password = os.getenv('PASSWORD'),
        database= os.getenv('DB')
        )
    _c = 1
    return conn

connection = connect()


# --------------------- HELPERS --------------------- #
txdatacontracts = ''' 

CREATE TABLE IF NOT EXISTS txdatacontracts(
    timestamp INTEGER,
    blocknumber INTEGER,
    gas INTEGER,
    gasPrice Bigint,
    _from TEXT,
    _to TEXT,
    contractAddress TEXT,
    value DECIMAL
    )

                  '''

def executeSql(sql):
    conn = connect()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    print('SQL executed')

def drop_table(table):
    conn = connect()
    cursor = conn.cursor()
    sql = f"DROP TABLE {table}"
    cursor.execute(sql)
    conn.commit()
    print('dropped')


# --------------------- DEF --------------------- #
def getLatestBlock():
    data = []
    block = w3.eth.get_block('latest') 
    timestamp = w3.eth.get_block(block.number).timestamp
    for tx_hash in block['transactions']:
        data.append(tx_hash)
    return data, timestamp


def processTx(tx_hash):
    try:
        tx = w3.eth.get_transaction(tx_hash)
        if tx['to'] == None:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            contractAddress = receipt['contractAddress']
        else:
            contractAddress = "" 
        tx_obj = { 'timestamp': timestamp, 'blocknumber': tx['blockNumber'], 'gas': tx['gas'], 'gasPrice': tx['gasPrice'],   'from': tx['from'], 'to': tx['to'], 'contractAddress': contractAddress, 'value': Web3.fromWei(tx['value'], 'ether')}
        return tx_obj
    except Exception as e:
        print(f"Error processing tx: {e}")
        tx_obj = { 'timestamp': 0, 'blocknumber': 0, 'gas': 0, 'gasPrice': 0,   'from': "", 'to': "", 'contractAddress': "", 'value': 0}
        return tx_obj


def dataToSql(data):
    global _c
    if _c == 0:
        global connection
        connection = connect()
    try:
        df = pd.DataFrame(data)
        df_ready = df.empty
        print(df)
        if df_ready != True:
            sio = StringIO()
            writer = csv.writer(sio)
            writer.writerows(df.values)
            sio.seek(0)
            with connection.cursor() as c:
                c.copy_from(
                    file=sio,
                    table="txdatacontracts",
                    columns=[
                        "timestamp",
                        "blocknumber",
                        "gas",
                        "gasprice",
                        "_from",
                        "_to",
                        "contractaddress",
                        "value"
                    ],
                    sep=","
                )
                connection.commit()
    except Exception as e:
        print(f"error while saving SQL: {e}")
        connection.close()
        _c = 0



# --------------------- Run and retry if fail --------------------- #
if __name__ == '__main__':
    #drop_table('txdata')
    #executeSql(txdatacontracts)
    while True:     
        try:
            data, timestamp = getLatestBlock()
            if ts != timestamp:
                print("===========================================///===============================================")
                ts = timestamp
                print(f"Fetching block @ timestamp: {timestamp}")
                pool = Pool()
                results = pool.map(processTx, data)
                print('Saving Data')
                dataToSql(results)
               
        except Exception as e:
            print(e)
            t =  str(pd.Timestamp(datetime.now()))
            print(f"restart: {t}")
            continue