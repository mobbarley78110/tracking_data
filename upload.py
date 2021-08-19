import pandas as pd
import numpy as np
import pyodbc
import datetime as dt
from functions import *

today = dt.datetime.today().strftime('%Y-%m-%d')
conn = pyodbc.connect('DRIVER=SQL Server;SERVER=QSQL')

def delete_all():
    cursor = conn.cursor()
    sql = ''' DELETE from tracking_data '''
    cursor.execute(sql)
    cursor.commit()
    return

def delete_blank_status():
    cursor = conn.cursor()
    sql = ''' DELETE from tracking_data WHERE status is NULL or status = '' or status = ' '  '''
    cursor.execute(sql)
    cursor.commit()

def upload(data):
    clean_for_upload(data)
    cursor = conn.cursor()
    for row in data.itertuples():
        sql = f""" INSERT INTO TRACKING_DATA   (TRACKING_NO,
                                                STATUS,
                                                SHIP_DATE,
                                                ESTIMATED_DELIVERY_DATE,
                                                DELIVERY_DATE,
                                                SIGNED_BY,
                                                ORIGIN,
                                                DESTINATION,
                                                LAST_UPDATE,
                                                CARRIER,
                                                SOURCE_TABLE,
                                                SOURCE_PK)
            VALUES ('{row.TRACKING_NO}',
                    '{row.STATUS}',
                    '{row.SHIP_DATE}',
                    '{row.ESTIMATED_DELIVERY_DATE}',
                    '{row.DELIVERY_DATE}',
                    '{row.SIGNED_BY}',
                    '{row.ORIGIN}',
                    '{row.DESTINATION}',
                    '{row.LAST_UPDATE}',
                    '{row.CARRIER}',
                    '{row.SOURCE_TABLE}',
                    '{row.SOURCE_PK}') """
        try:
            cursor.execute(sql)
        except:
            print('FAILED UPLOAD')
            print(row)
    cursor.commit()
    clean_dates()
    return

def upload_csv(url):
    data = pd.read_csv(url)
    upload(data)
    return

def clean_dates():
    cursor = conn.cursor()
    cursor.execute(''' UPDATE tracking_data
        SET ship_date = NULL
        WHERE ship_date = '1900-01-01'
    ''')
    cursor.execute(''' UPDATE tracking_data
        SET estimated_delivery_date = NULL
        WHERE estimated_delivery_date = '1900-01-01'
    ''')
    cursor.execute(''' UPDATE tracking_data
        SET delivery_date = NULL
        WHERE delivery_date = '1900-01-01'
    ''')
    cursor.execute(''' UPDATE tracking_data
        SET DESTINATION = NULL
        WHERE DESTINATION = ', , '
    ''')
    cursor.execute(''' UPDATE tracking_data
        SET ORIGIN = NULL
        WHERE ORIGIN = ', , '
    ''')

    conn.commit()
    return

def fix_last_update():
    cursor = conn.cursor()
    sql = f''' UPDATE tracking_data
              SET last_update = {today}
              WHERE last_update = '1900-01-01'  '''
    cursor.execute(sql)
    cursor.commit()
    return

def pickle_to_excel(url):
    data = pd.read_pickle(url)
    data.to_excel(url.replace('.pkl','.xlsx'), index = False)
    return

def pickle_to_csv(url):
    data = pd.read_pickle(url)
    data.to_csv(url.replace('.pkl','.csv'), index = False)
    return

def download_clean_dups_reup():
    data = pd.read_sql(''' SELECT * FROM TRACKING_DATA  ''', conn)
    data.drop_duplicates(subset = ['TRACKING_NO','SOURCE_TABLE','SOURCE_PK'],
                         keep = 'last',
                         inplace = True)
    clean_for_upload(data)
    delete_all()
    upload(data)

def delete_e_lines():
    cursor = conn.cursor()
    sql = ''' delete tracking_data where tracking_no like '%E+11'  '''
    cursor.execute(sql)
    cursor.commit()
    return

def upload_carrier():
    data = pd.read_csv('data/carrier.csv')
    print(data.head())
    cursor = conn.cursor()
    for row in data.itertuples():
        sql = f"""
        UPDATE tracking_data
        SET carrier = '{row.CARRIER}'
        WHERE source_table = '{row.SOURCE_TABLE}'
        and source_pk = '{row.SOURCE_PK}'
        and tracking_no = '{row.TRACKING_NO}'
        """
        cursor.execute(sql)
        cursor.commit()
    return


def create_df_for_one_ups_awb(track_no):
    df = pd.DataFrame([[track_no,'']], columns = ['TRACKING_NO','STATUS'] )
    df['CLEAN_TRACKING_NO'] = df['TRACKING_NO'].apply(clean_awb)
    return df


run_ups_batch(create_df_for_one_ups_awb('1Z3424840340794246'))
