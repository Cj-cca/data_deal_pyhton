# !/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import time
import json
import zlib
import sys
import re
import pandas
import requests
import base64
from sqlalchemy import create_engine
from sqlalchemy import text

notNullField = 'Worker_ID'
FieldsDate = ['Effective_Date', 'Date_Time_Initiated', 'Date_Time_Completed']

fieldMapping = {'Worker_ID': 'Worker_ID', 'WID': 'WID', 'Worker_Name': 'Worker_Name',
                'Business_Process_Event': 'Business_Process_Event', 'Business_Process_Name': 'Business_Process_Name',
                'Business_Process_Type': 'Business_Process_Type', 'Business_Process_Reason': 'Business_Process_Reason',
                'Business_Process_Reason_Category': 'Business_Process_Reason_Category',
                'Effective_Date': 'Effective_Date', 'Subject': 'Subject', 'Transaction_Status': 'Transaction_Status',
                'Date_Time_Initiated': 'Date_Time_Initiated', 'Initiator': 'Initiator',
                'Date_Time_Completed': 'Date_Time_Completed'}

resultColumn_ods = ['page_index',
                    'create_time',
                    'response_data',
                    'response_object_count',
                    'query_url'
                    ]

resultColumn_ods_detail = ['batch_id',
                           'create_time',
                           'current_page_num',
                           'response_data',
                           'response_object_num',
                           'query_url',
                           'comment'
                           ]

TerminationAndTransferTableNameDwd = "TerminationAndTransfer_dwd"
TerminationAndTransferTableNameOds = "TerminationAndTransfer_ods"
TerminationAndTransferTableNameOdsDetail = "TerminationAndTransfer_ods_detail"

tarEngine = create_engine('mysql+pymysql://root@10.158.16.244:9030/WorkDayStage')
batchID = 0
createTime = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def syncApiData(user_name, password, query_url, page_index=1, start_time='', end_time=''):
    credentials = f"{user_name}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    auth = f"Basic {encoded_credentials}"
    if start_time == '':
        headers = {
            "Authorization": auth,
            "beginDateTime": start_time,
            "endDateTime": end_time
        }
    else:
        headers = {
            "Authorization": auth
        }
    proxies = requests.utils.getproxies()
    if proxies and 'https' in proxies:
        proxies['https'] = proxies['http']
    print(f"start sync data from page {page_index}")
    start = time.time()
    response = requests.get(query_url, headers=headers, proxies=proxies, verify=False).json()
    deal_ods(response, query_url, page_index)
    deal_ods_detail(response, query_url, page_index)
    deal_dwd(response)
    print(f"page {page_index} data sync complete, cost {round(time.time() - start, 2)}s")
    if "__next" in response and response["__next"] is not None:
        next_url = response["__next"]
        page_index += 1
        syncApiData(user_name, password, next_url, page_index=page_index)


def deal_ods_detail(response, query_url, page_index=1):
    global batchID
    dataNumber = 0
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods_detail)
    for value in response:
        batchID += 1
        dataNumber += 1
        comment = "" if value[f"{notNullField}"] is not None else "Exception data， externalCode is None"
        tmp_dataframe.loc[len(tmp_dataframe.index)] = [batchID, createTime, page_index,
                                                       json.dumps(value), dataNumber, query_url, comment]
    insert_count = tmp_dataframe.to_sql(TerminationAndTransferTableNameOdsDetail, tarEngine, if_exists='append',
                                        index=False)
    print(f"{TerminationAndTransferTableNameOdsDetail}数据插入成功，受影响行数：", insert_count)


def deal_ods(response, query_url, page_index):
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods)
    originalParentCount = len(response)
    str_response = json.dumps(response).encode('utf-8')
    compressed_response = zlib.compress(str_response)
    compressed_response_str = base64.b64encode(compressed_response).decode('utf-8')
    tmp_dataframe.loc[len(tmp_dataframe.index)] = [page_index, createTime, compressed_response_str,
                                                   originalParentCount, query_url]
    insert_count = tmp_dataframe.to_sql(TerminationAndTransferTableNameOds, tarEngine, if_exists='append', index=False)
    print(f"{TerminationAndTransferTableNameOds}数据插入成功，受影响行数：", insert_count)


def deal_date_list(date_list):
    dates = []
    for date in date_list.split(','):
        dt = format_date(date).strip()
        if dt != '':
            dates.append(dt)
    return ','.join(dates)


# 将date:2021-12-20T00:00:00.000格式化为：1/1/0001 12:00:00 AM
def format_date(date):
    result = ''
    if date == '' or date == 'null':
        return result
    if len(date) > 18:
        date_obj = datetime.datetime.fromisoformat(date)
        result = date_obj.strftime('%m/%d/%Y %I:%M:%S %p')
    elif len(date) == 4:
        date = f"{date}-01-01T12:00:00.000"
        date_obj = datetime.datetime.fromisoformat(date)
        result = date_obj.strftime('%m/%d/%Y %I:%M:%S %p')
    else:
        print("Exception: unknown date format")
    return result


def format_fields_date(dataframe, index):
    for field_date in FieldsDate:
        dataframe.loc[index, field_date] = deal_date_list(dataframe.loc[index, field_date])


def deal_dwd(response):
    data_frame = pandas.json_normalize(response)
    data_frame.rename(columns=fieldMapping, inplace=True)
    result_dataframe = data_frame.fillna('')
    for i in range(0, len(result_dataframe.index)):
        format_fields_date(result_dataframe, i)
    result_dataframe = result_dataframe[list(fieldMapping.values())]
    insert_count = result_dataframe.to_sql(TerminationAndTransferTableNameDwd, tarEngine, if_exists='append', index=False)
    print(f"{TerminationAndTransferTableNameDwd}数据插入成功，总数据条数: {len(result_dataframe)}，插入行数：{insert_count}")


def truncateTable(engine, table_name):
    tarConn = engine.connect()
    tarConn.execute(text(f"truncate table {table_name}"))
    tarConn.close()
    print("table truncate is complete")


if __name__ == '__main__':
    startTimeStr = '2023-07-21'
    endTimeStr = '2023-07-31'
    user = "sb-a8531244-374b-414c-8944-0dbdf941c2e5!b1813|it-rt-pwc-dev!b39"
    pwd = "f4aee3e5-9539-4568-be98-404a5c6ca253$yxW2FNy_fKA8a1Fjn44SM3zjSt4VGvIbzu9tQnHfWdg="
    termination_and_transfer_url = "https://pwc-dev.it-cpi010-rt.cpi.cn40.apps.platform.sapcloud.cn/http/vprofile/transfer"
    if startTimeStr == '':
        syncApiData(user, pwd, termination_and_transfer_url)
    else:
        startTime = datetime.datetime.strptime(startTimeStr, "%Y-%m-%d")
        endTime = datetime.datetime.strptime(endTimeStr, "%Y-%m-%d")
        while startTime < endTime:
            tmpStartTime = startTime
            tmpEndTime = tmpStartTime + datetime.timedelta(days=1)
            createTime = tmpEndTime
            syncApiData(user, pwd, termination_and_transfer_url, start_time=tmpStartTime.strftime("%Y-%m-%dT%H:%M:%S"),
                        end_time=tmpEndTime.strftime("%Y-%m-%dT%H:%M:%S"))
            startTime += datetime.timedelta(days=1)
