import gc
import math
import pyodbc
import pandas as pd
from os import name
import urllib
from sqlalchemy import create_engine

'''reading parquet files from local folder and then converting it into pandas dataframe, `df = df.convert_dtypes()` used to converts the datatypes
  of DataFrame columns to the appropriate pandas extention type. it helps optimize memory usage and improves performance. `pyarrow` engine is the
  part of the Apache Arrow project, and it provides efficient and high-performance reading and writing of parquet files. `sqlalchemy` is a python
  SQL toolkit and Object-Relational Mapping(ORM) library. It provides a set of high-level API for connecting to relational databases, executing 
  SQL queries, and managing the database schema.
  '''

def readParquet():
    parquetAllFolder = r'C:\Users\KumarGaurav\Documents\IBM Projects\VHR\VHRFiles\000030\year=2022\month=12\day=20\hour=0'
    print("Reading all parquet file is in progress .... ....")
    df = pd.read_parquet(parquetAllFolder, engine='pyarrow')
    df = df.convert_dtypes()
    print("parquet file stored in dataframe")
    return df


def connectToDb():
    server = "parquet.database.windows.net"
    database = "praquet"
    username = "sql"
    password = "Enter_Password_here"
    print("staring connection .....")
    driver = '{ODBC Driver 18 for SQL Server}'
    odbc_str = 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;UID=' + username + ';DATABASE=' + database + ';PWD=' + password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine = create_engine(connect_str, fast_executemany=True)
    print("connection successful")
    return engine

def processDataframe(df):
    longFormatDf = df.melt(
    id_vars=['DAQ_ID', 'VIN', 'Notification_Feature', 'DAQ_EventOccurTime_UTC', 'One_Time_UUID', 'Product_Own_ID',
             'Event_ID'], var_name='def', value_name='value')
    del df
    gc.collect()
    longFormatDf['DataId'] = 'NULL'
    longFormatDf['Unit'] = 'NULL'
    longFormatDf['Filename'] = 'NULL'
    longFormatDf.rename(
        columns={'DAQ_EventOccurTime_UTC': 'UploadDate',
             'Notification_Feature': 'DataDesc', 'DAQ_ID': 'DAQId'},
            inplace=True)
    # longFormatDf['CreatedBy']='dbo'
    # finalLongFormat = longFormatDf.sort_values('VIN')
    l = len(longFormatDf.index)
    batch = 400000
    a = math.ceil(l / batch)
    return longFormatDf, a, batch


def saveToDb(longFormatDf, a, batch, engine):
    for i in range(0, a + 1):
        newDataFrame = longFormatDf.iloc[i * batch:(i + 1) * batch, :]
        newDataFrame.to_sql(
            name='DAQUploadStg_T1',
            con=engine,
            if_exists='append',
            index=False,
            schema="dbo")
        print("Iteration no. {} completed".format(i))
        print("Data Process is in progress, expected time to complete {} minutes".format((a * 22 - a * i) / 60))


if __name__ == '__main__':
    df = readParquet()
    engine = connectToDb()
    longFormatDf, a, batch = processDataframe(df)
    saveToDb(longFormatDf, a, batch, engine)
    print("Insertion is successful")

