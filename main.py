import re
import requests
import datetime
import pandas as pd
import pyodbc
import numpy as np
import time
import datetime as dt
from functions import *

today = dt.datetime.today()
conn = pyodbc.connect('DRIVER=SQL Server;SERVER=QSQL;')
pd.options.mode.chained_assignment = None

# # pull all existing data from QSQL
print('pulling current data')
sql = ''' SELECT * FROM tracking_data '''
tracking_data = pd.read_sql(sql, conn)
tracking_data['CLEAN_TRACKING_NO'] = tracking_data['TRACKING_NO'].apply(clean_awb)
print('total data lenght: ' + str(len(tracking_data)))
# create list of awb to update for Fedex and run batch
update_fdx = tracking_data[(~tracking_data['STATUS'].isin(['Delivered','no data found','Cancelled']))
                            & (tracking_data['CARRIER'] == 'FEDEX')]
#_update_fdx = update_fdx[['CLEAN_TRACKING_NO','STATUS']].copy()
#_update_fdx.drop_duplicates(subset ='CLEAN_TRACKING_NO', inplace = True)
print('updating ' + str(len(update_fdx)) + ' fedex records')

run_fedex_batch(update_fdx)

# same with UPS
update_ups = tracking_data[(tracking_data['STATUS'] != 'Delivered')
                            & (tracking_data['STATUS'] != 'no data found')
                            & (tracking_data['STATUS'] != 'Cancelled')
                            & (tracking_data['CARRIER'] == 'UPS')]

#_update_ups = update_ups[['CLEAN_TRACKING_NO','STATUS']].copy()
#_update_ups.drop_duplicates(subset ='CLEAN_TRACKING_NO', inplace = True)
print('updating ' + str(len(update_ups)) + ' ups records')
run_ups_batch(update_ups)

update_upload = update_fdx.append(update_ups)
clean_for_upload(update_upload)

# update data in QSQL
cursor = conn.cursor()
for row in update_upload.itertuples():
    sql = f""" UPDATE tracking_data
               SET
                status = '{row.STATUS}',
                ship_date = '{row.SHIP_DATE}',
                estimated_delivery_date = '{row.ESTIMATED_DELIVERY_DATE}',
                delivery_date = '{row.DELIVERY_DATE}',
                signed_by = '{row.SIGNED_BY}',
                origin = '{row.ORIGIN}',
                destination = '{row.DESTINATION}',
                last_update = '{row.LAST_UPDATE}'
               WHERE tracking_no = '{row.TRACKING_NO}' and source_table = '{row.SOURCE_TABLE}' and  source_pk = '{row.SOURCE_PK}' """
    cursor.execute(sql)
cursor.commit()

# now pull new AWB from QSQL
print('pull new awb')
# Create list of new AWB, pulling from all known TRACKING_NUMBER in Quantum.
# Somehow some tables have a TRACKING_NUMBER, some others an AIRWAY_BILL,
# some have both...
# We pull data from both header and details for each category:
# - shippers:
# - receivers:
# - po:
# - ro:
# - invoices:   both
# - stock:      airwaybill
# - exchanges:  tracking
# we are not pulling data from:
# - so (no awb fields found)
new_awb = pd.DataFrame()

 # shippers (header only)
sql_query = '''
SELECT DISTINCT TRACKING_NUMBER as TRACKING_NO,
SMH_AUTO_KEY as SOURCE_PK,
'SM_HEADER' as SOURCE_TABLE
FROM SM_HEADER WHERE DATE_CREATED > '2021-01-01' '''
new_awb = new_awb.append(pd.read_sql(sql_query, conn))

# RO
sql_query = '''
SELECT DISTINCT TRACKING_NUMBER as TRACKING_NO,
ROH_AUTO_KEY as SOURCE_PK,
'RO_HEADER' as SOURCE_TABLE
FROM RO_HEADER WHERE ENTRY_DATE > '2021-01-01' '''
new_awb = new_awb.append(pd.read_sql(sql_query, conn))

sql_query = '''
SELECT DISTINCT TRACKING_NUMBER as TRACKING_NO,
ROD_AUTO_KEY as SOURCE_PK,
'RO_DETAIL' as SOURCE_TABLE
FROM RO_DETAIL WHERE ENTRY_DATE > '2021-01-01' '''
new_awb = new_awb.append(pd.read_sql(sql_query, conn))

# PO
sql_query = '''
SELECT DISTINCT TRACKING_NUMBER as TRACKING_NO,
POH_AUTO_KEY as SOURCE_PK,
'PO_HEADER' as SOURCE_TABLE
FROM PO_HEADER WHERE ENTRY_DATE > '2021-01-01' '''
new_awb = new_awb.append(pd.read_sql(sql_query, conn))

sql_query = '''
SELECT DISTINCT TRACKING_NUMBER as TRACKING_NO,
POD_AUTO_KEY as SOURCE_PK,
'PO_DETAIL' as SOURCE_TABLE
FROM PO_DETAIL WHERE ENTRY_DATE > '2021-01-01' '''
new_awb = new_awb.append(pd.read_sql(sql_query, conn))

# Receivers
sql_query = '''
SELECT DISTINCT AIRWAY_BILL as TRACKING_NO,
RCH_AUTO_KEY as SOURCE_PK,
'RC_HEADER' as SOURCE_TABLE
FROM RC_HEADER WHERE DATE_CREATED > '2021-01-01' '''
new_awb = new_awb.append(pd.read_sql(sql_query, conn))

sql_query = '''
SELECT DISTINCT AIRWAY_BILL as TRACKING_NO,
RCD_AUTO_KEY as SOURCE_PK,
'RC_DETAIL' as SOURCE_TABLE
FROM RC_DETAIL WHERE ENTRY_DATE > '2021-01-01' '''
new_awb = new_awb.append(pd.read_sql(sql_query, conn))

# Invoices (nothing in details - both both tracking and airwaybill in header)
sql_query = '''
SELECT DISTINCT TRACKING_NUMBER as TRACKING_NO,
INH_AUTO_KEY as SOURCE_PK,
'INVC_HEADER' as SOURCE_TABLE
FROM INVC_HEADER WHERE DATE_CREATED > '2021-01-01' '''
new_awb = new_awb.append(pd.read_sql(sql_query, conn))

sql_query = '''
SELECT DISTINCT AIRWAY_BILL as TRACKING_NO,
INH_AUTO_KEY as SOURCE_PK,
'INVC_HEADER' as SOURCE_TABLE
FROM INVC_HEADER WHERE DATE_CREATED > '2021-01-01' '''
new_awb = new_awb.append(pd.read_sql(sql_query, conn))

# stock
sql_query = '''
SELECT DISTINCT AIRWAY_BILL as TRACKING_NO,
STM_AUTO_KEY as SOURCE_PK,
'STOCK' as SOURCE_TABLE
FROM STOCK WHERE REC_DATE > '2021-01-01' '''
new_awb = new_awb.append(pd.read_sql(sql_query, conn))

# exchanges
sql_query = '''
SELECT DISTINCT CORE_TRACKING_NUMBER as 'TRACKING_NO',
EXC_AUTO_KEY as SOURCE_PK,
'EXCHANGE' as SOURCE_TABLE
FROM EXCHANGE WHERE CORE_SHIP_DATE > '2021-01-01' '''
new_awb = new_awb.append(pd.read_sql(sql_query, conn))

# Clean up new data and drop the ones we already have in QSQL at source_pk level
new_awb = new_awb[~new_awb['TRACKING_NO'].isna()]
new_awb = new_awb[new_awb['TRACKING_NO'] != '']
new_awb['CLEAN_TRACKING_NO'] = new_awb['TRACKING_NO'].apply(clean_awb)
new_awb.drop_duplicates(inplace = True)
new_awb = new_awb.merge(tracking_data[['TRACKING_NO','SOURCE_PK','SOURCE_TABLE']], how = 'left', indicator=True)
new_awb = new_awb[new_awb['_merge'] == 'left_only']
new_awb.drop(columns = '_merge', inplace = True)

# bring the ones we already have so we dont search on those again at awb level
_existing = tracking_data.copy()
_existing.drop(columns = ['TRACKING_NO','SOURCE_PK','SOURCE_TABLE'], inplace = True)
_existing.drop_duplicates(subset = 'CLEAN_TRACKING_NO', inplace = True)
_list_existing_awb = _existing['CLEAN_TRACKING_NO'].unique()

# create a temp table with unique awb's - the clean AWB wil serve as index
_new_awb = new_awb[['CLEAN_TRACKING_NO']].copy()
_new_awb.drop_duplicates(inplace = True)
_new_awb = _new_awb[~_new_awb['CLEAN_TRACKING_NO'].isin(_list_existing_awb)]

# test if those awb match fedex or ups awb templates and assign a status
_new_awb['VALID_AWB'] = _new_awb['CLEAN_TRACKING_NO'].apply(test_awb)
_new_awb['STATUS'] = None

print('new awb to run')
print(_new_awb['VALID_AWB'].value_counts())

# create a table with the no match AWBs
_new_no_match = _new_awb[_new_awb['VALID_AWB'] == 'no match'][['CLEAN_TRACKING_NO','STATUS']]
_new_no_match['STATUS'] = 'no data found'
_new_no_match['LAST_UPDATE'] = today

# run fedex on the good new ones only + the ones to review
_new_fedex = _new_awb[_new_awb['VALID_AWB'] == 'fedex'][['CLEAN_TRACKING_NO','STATUS']]
print('pulling new FEDEX data on ' + str(len(_new_fedex)) + ' records')
if len(_new_fedex) > 0:
    run_fedex_batch(_new_fedex)

# now do the same thing on UPS
_new_ups = _new_awb[_new_awb['VALID_AWB'] == 'ups'][['CLEAN_TRACKING_NO','STATUS']]
print('pulling new UPS data on ' + str(len(_new_ups)) + ' records')
if len(_new_ups) > 0:
    run_ups_batch(_new_ups)

# append the 4 results tables and merge to main new_awb table
_new_awb = _new_fedex.append(_new_ups).append(_new_no_match).append(_existing)
_new_awb.drop_duplicates(inplace = True)

new_awb = new_awb.merge(_new_awb,
                        left_on = 'CLEAN_TRACKING_NO',
                        right_on = 'CLEAN_TRACKING_NO',
                        how = 'left')
new_awb.drop_duplicates(inplace = True)

# do the SQL INSERT
clean_for_upload(new_awb)

cursor = conn.cursor()
for row in new_awb.itertuples():
    sql = f""" INSERT INTO TRACKING_DATA   (TRACKING_NO,
                                            STATUS,
                                            SHIP_DATE,
                                            ESTIMATED_DELIVERY_DATE,
                                            DELIVERY_DATE,
                                            SIGNED_BY, ORIGIN,
                                            DESTINATION,
                                            LAST_UPDATE,
                                            SOURCE_TABLE,
                                            SOURCE_PK,
                                            CARRIER)
        VALUES ('{row.TRACKING_NO}',
                '{row.STATUS}',
                '{row.SHIP_DATE}',
                '{row.ESTIMATED_DELIVERY_DATE}',
                '{row.DELIVERY_DATE}',
                '{row.SIGNED_BY}',
                '{row.ORIGIN}',
                '{row.DESTINATION}',
                '{row.LAST_UPDATE}',
                '{row.SOURCE_TABLE}',
                '{row.SOURCE_PK}',
                '{row.CARRIER}') """
    try:
        cursor.execute(sql)
    except:
        print(row)
    cursor.commit()

# cleaning 1900-01-01 dates
clean_dates()
