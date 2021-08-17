import requests
import datetime
import pandas as pd
import pyodbc
import numpy as np
import time
import datetime as dt
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import re

today = dt.datetime.today().strftime('%Y-%m-%d')
pd.options.mode.chained_assignment = None
conn = pyodbc.connect('DRIVER=SQL Server;SERVER=QSQL')


def delete_all():
    cursor = conn.cursor()
    sql = ''' DELETE from tracking_data '''
    cursor.execute(sql)
    cursor.commit()
    return


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
    return


def download_clean_dups_reup():
    data = pd.read_sql(''' SELECT * FROM TRACKING_DATA  ''', conn)
    data.drop_duplicates(subset = ['TRACKING_NO','SOURCE_TABLE','SOURCE_PK'],
                         keep = 'last',
                         inplace = True)
    clean_for_upload(data)
    delete_all()
    upload(data)


def test_awb(awb):
    # fedex awb are 10, 12, or 34 digit long numbers
    # ups awb are 18 digits long and start with '1Z'
    if re.match(r'^[0-9]{10}$', awb):
        return 'fedex'
    elif re.match(r'^[0-9]{12}$', awb):
        return 'fedex'
    elif re.match(r'^[0-9]{34}$', awb):
        return 'fedex'
    elif (awb[0:2] == '1Z') & (len(awb) == 18):
        return 'ups'
    else:
        return 'no match'
    return


def clean_awb(awb):
    awb = re.sub(r'[^a-zA-Z0-9]', '', awb)
    words_to_remove = ['SCRAP','CPU', 'DEL', 'NOAWB','AWB','FDX','FEDEX','UPS','GRND','FROMMIA','FROMAERO','EXCHANGE','UPS GRND']
    awb = re.sub(r'|'.join(words_to_remove), '', awb)
    awb = awb.upper()
    return awb


def clean_for_upload(df):
    df['SHIP_DATE'] = pd.to_datetime(df['SHIP_DATE'], errors = 'coerce').dt.strftime('%Y-%m-%d')
    df['ESTIMATED_DELIVERY_DATE'] = pd.to_datetime(df['ESTIMATED_DELIVERY_DATE'], errors = 'coerce').dt.strftime('%Y-%m-%d')
    df['DELIVERY_DATE'] = pd.to_datetime(df['DELIVERY_DATE'], errors = 'coerce').dt.strftime('%Y-%m-%d')
    df['LAST_UPDATE'] = pd.to_datetime(df['LAST_UPDATE'], errors = 'coerce').dt.strftime('%Y-%m-%d')
    df.replace([pd.NaT], '', inplace = True)
    df.replace([np.nan], '', inplace = True)
    df['ORIGIN'] = df['ORIGIN'].str.replace("'", '')
    df['DESTINATION'] = df['DESTINATION'].str.replace("'", '')
    return


def get_package_details(track_no):
    try:
        header = {
            'Origin': 'https://www.fedex.com',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/59.0.3071.115 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': '*/*',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://www.fedex.com/apps/fedextrack/?tracknumbers=%s&locale=en_CA&cntry_code=ca_english' % (
                str(track_no)),
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.8,fr;q=0.6,ta;q=0.4,bn;q=0.2'
        }

        data = {
            'action': 'trackpackages',
            'data': '{"TrackPackagesRequest":{"appType":"WTRK","appDeviceType":"DESKTOP","uniqueKey":"",'
                    '"processingParameters":{},"trackingInfoList":[{"trackNumberInfo":{"trackingNumber":"%s",'
                    '"trackingQualifier":"","trackingCarrier":""}}]}}' % (
                        str(track_no)),
            'format': 'json',
            'locale': 'en_CA',
            'version': '1'
        }

        url = "https://www.fedex.com/trackingCal/track"

        response = requests.post(url, data=data, headers=header)

        if response.status_code == 200:
            pass
        else:
            return

        res_json = response.json()

        if res_json['TrackPackagesResponse']['packageList'][0]['errorList'][0]['message'] != "":
            # exits the function if package id is wrong
            return

        result = {
            'tracking_no': int(track_no),
            'ship_date': res_json['TrackPackagesResponse']['packageList'][0]['displayPickupDt'],
            'status': res_json['TrackPackagesResponse']['packageList'][0]['keyStatus'],
            'scheduled_delivery': res_json['TrackPackagesResponse']['packageList'][0]['displayEstDeliveryDt'],
            'delivery_date':res_json['TrackPackagesResponse']['packageList'][0]['displayActDeliveryDt'],
            'signed_by':res_json['TrackPackagesResponse']['packageList'][0]['receivedByNm'],
            'dest_city':res_json['TrackPackagesResponse']['packageList'][0]['destLocationCity'],
            'dest_state':res_json['TrackPackagesResponse']['packageList'][0]['destLocationStateCD'],
            'dest_zip':res_json['TrackPackagesResponse']['packageList'][0]['destLocationZip'],
            'dest_country':res_json['TrackPackagesResponse']['packageList'][0]['destLocationCntryCD'],
            'orig_city':res_json['TrackPackagesResponse']['packageList'][0]['originCity'],
            'orig_state':res_json['TrackPackagesResponse']['packageList'][0]['originStateCD'],
            'orig_zip':res_json['TrackPackagesResponse']['packageList'][0]['originZip'],
            'orig_country':res_json['TrackPackagesResponse']['packageList'][0]['originCntryCD']
        }
        return result

    except Exception as e:
        print(f'Error occurred on awb: {track_no}. \n Error Message : ' + str(e))
        pass


def run_fedex_batch(df):
    for row in df.itertuples():
        track_id = row.CLEAN_TRACKING_NO
        #print(track_id + ':')
        current_status = row.STATUS
        index = row.Index
        if (current_status != 'Delivered') & (current_status != 'no data found') & (current_status != 'Cancelled'):
            temp_dict =  get_package_details(track_id)
            if temp_dict is not None:
                df.loc[index, 'SHIP_DATE'] = temp_dict['ship_date']
                df.loc[index, 'STATUS'] = temp_dict['status']
                df.loc[index, 'ESTIMATED_DELIVERY_DATE'] = temp_dict['scheduled_delivery']
                df.loc[index, 'DELIVERY_DATE'] = temp_dict['delivery_date']
                df.loc[index, 'SIGNED_BY'] = temp_dict['signed_by']
                df.loc[index, 'DESTINATION'] = temp_dict['dest_city'] + ', ' + temp_dict['dest_state'] + temp_dict['dest_zip'] + ', ' + temp_dict['dest_country']
                df.loc[index, 'ORIGIN'] = temp_dict['orig_city'] + ', ' + temp_dict['orig_state'] + temp_dict['orig_zip'] + ', ' + temp_dict['orig_country']
                df.loc[index, 'LAST_UPDATE'] = today
                df.loc[index, 'CARRIER'] = 'FEDEX'
                #print('\t'+temp_dict['status'])
            else: # if not data returned, change it to "no data" only if it was a new record
                if current_status is None:
                    df.loc[index, 'STATUS'] = 'no data found'
                    df.loc[index, 'LAST_UPDATE'] = today
                    #print('\tno data found')
                else: # otherwise keep old status
                    df.loc[index, 'STATUS'] = current_status
                    #print('\t'+current_status)

    return


def run_ups_batch(df):
    driver = webdriver.Chrome('chromedriver.exe')
    for row in df.itertuples():
        prev_status = row.STATUS
        awb = row.CLEAN_TRACKING_NO
        index = row.Index
        #print(f'testing {awb}')
        if ('Z' in awb) & (prev_status != 'no data found') & (prev_status != 'Delivered'):
            # open UPS url
            try:
                driver.get(f'https://www.ups.com/track?loc=null&tracknum={awb}&requester=WT/trackdetails')
            except TimeoutException:
                print(f'TimeoutException on awb: {awb}' )

                continue

            # figure out what status we are in
            attempt_status = ''
            # for NOT A GOOD AWB: look for id='stApp_error_alert_list0'
            try:
                element = WebDriverWait(driver, 1).until(
                    EC.presence_of_element_located((By.ID, 'stApp_error_alert_list0'))
                    )
                attempt_status = driver.find_element_by_id('stApp_error_alert_list0').text
                attempt_status = 'no data found'
                df.loc[index, 'STATUS'] = attempt_status
                df.loc[index, 'LAST_UPDATE'] = today
                continue
            except:
                pass
            # For DELIVERED: look for id ='st_App_DelvdLabel'
            try:
                attempt_status = driver.find_element_by_id('st_App_DelvdLabel').text
                attempt_status = 'Delivered'
            except:
                pass
            # For ON THE WAY: look for id='st_App_EstDelLabel'
            try:
                attempt_status = driver.find_element_by_id('st_App_EstDelLabel').text
                attempt_status = 'In transit'
            except:
                pass
            # For CANCELLED
            # For LABEL CREATED

            if attempt_status == '':
                if prev_status is None:
                    attempt_status = 'no data found'
                    df.loc[index, 'STATUS'] = attempt_status
                    df.loc[index, 'LAST_UPDATE'] = today
                    continue
                else:
                    continue

            # ----- DELIVERED
            if attempt_status == 'Delivered':
                #dest_city = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.ID, "stApp_txtAddress")))
                dest_city = driver.find_element_by_id('stApp_txtAddress').text
                dest_country = driver.find_element_by_id('stApp_txtCountry').text
                #del_date = driver.find_element_by_id('st_App_PkgStsMonthNum').text
                #status = driver.find_element_by_id('st_App_PkgStsLoc').text
                signed_by = driver.find_element_by_id('stApp_valReceivedBy').text
                #statuses = []
                #for i in range(0,20):
                #    try:
                #        statuses.append(driver.find_element_by_id(f'stApp_ShpmtProg_LVP_milestone_name_{i}').text)
                #    except:
                #        continue
                #last_status = statuses[-1]

                # click on More Details to get some extra info
                more_details = driver.find_element_by_id('st_App_View_Details')
                try:
                    driver.execute_script("window.scrollTo(0, 100)")
                    more_details.click()
                except:
                    time.sleep(0.4)
                    more_details.click()

                try:
                    ship_date = driver.find_element_by_id('stApp_txtAdditionalInfoBilledOn').text
                except:
                    ship_date = ''
                service = driver.find_element_by_id('stApp_link_AdditionalInfoService').text

                # clikc on the next tab
                next_tab = driver.find_element_by_id('tab_1')
                next_tab.click()
                shipped_from = ''
                for i in range(0,50):
                    try:
                        milestone_name = driver.find_element_by_id(f'stApp_milestoneName{i}').text
                        if milestone_name == 'Shipped':
                            shipped_from = driver.find_element_by_id(f'stApp_milestoneActivityLocation{i}').text #TODO: customs clearance issue
                            if shipped_from.contains('UPS is preparing your package for clearance'):
                                shipped_from = ''
                            if ship_date == '':
                                ship_date = driver.find_element_by_id(f'stApp_activitiesdateTime{i}').text
                        if milestone_name == 'Delivered':
                            del_date = driver.find_element_by_id(f'stApp_activitiesdateTime{i}').text
                    except:
                        continue

                # clean up some fields
                shipped_from = shipped_from.replace('Shipped','')
                shipped_from = shipped_from.replace('\n','')
                if 'clearance' in shipped_from:
                    shipped_from = ''

                # save all data in the dataTable
                df.loc[index, 'STATUS'] = 'Delivered'
                df.loc[index, 'SHIP_DATE'] = ship_date
                df.loc[index, 'DELIVERY_DATE'] = del_date.split()[0]
                df.loc[index, 'SIGNED_BY'] = signed_by.split()[0]
                df.loc[index, 'ORIGIN'] = shipped_from
                df.loc[index, 'DESTINATION'] = dest_city + ', ' + dest_country
                df.loc[index, 'LAST_UPDATE'] = today
                df.loc[index, 'CARRIER'] = 'UPS'

            # ---- IN TRANSIT
            if attempt_status == 'In transit':
                del_date = ''
                dest_city = '' #driver.find_element_by_id('stApp_txtAddress').text
                dest_country = '' #driver.find_element_by_id('stApp_txtCountry').text
                try:
                    est_delivery_date = driver.find_element_by_id('st_App_PkgStsTimeDayMonthNum').text
                except:
                    est_delivery_date = ''
                    pass
                # if est_delivery_date == 'Pending':
                #     est_delivery_date == ''
                # click on More Details to get some extra info
                try:
                    driver.execute_script("window.scrollTo(0, 100)")
                    more_details.click()
                except:
                    time.sleep(0.4)
                    try:
                        more_details.click()
                    except:
                        df.loc[index, 'STATUS'] = 'Pending'
                        df.loc[index, 'CARRIER'] = 'UPS'
                        df.loc[index, 'LAST_UPDATE'] = today
                        continue

                try:
                    ship_date = driver.find_element_by_id('stApp_txtAdditionalInfoBilledOn').text
                except:
                    ship_date = ''
                service = driver.find_element_by_id('stApp_link_AdditionalInfoService').text

                # save all data in the dataTable
                df.loc[index, 'STATUS'] = 'In transit'
                df.loc[index, 'ESTIMATED_DELIVERY_DATE'] = est_delivery_date
                df.loc[index, 'SHIP_DATE'] = ship_date
                df.loc[index, 'DESTINATION'] = dest_city + ', ' + dest_country
                df.loc[index, 'LAST_UPDATE'] = today
                df.loc[index, 'CARRIER'] = 'UPS'
        else:
            print('not a good AWB to try')
            df.loc[index, 'STATUS'] = 'no data found'
            df.loc[index, 'LAST_UPDATE'] = today

    driver.close()
    return
