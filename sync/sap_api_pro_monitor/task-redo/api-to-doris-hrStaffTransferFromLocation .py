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
from urllib.parse import quote_plus as urlquote

notNullField = 'Worker_ID'
FieldsDate = ['effective_date']
notNullFieldList = ['worker_id', 'effective_date', 'location_name']
fieldMapping = {'Worker_ID': 'worker_id', 'Effective_Date': 'effective_date', 'Location_Name': 'location_name'}

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
exceptionDataColumn_ods = ['unique_code', 'create_date', 'api_name', 'exception_data', 'exception_field',
                           'exception_type']

apiName = 'HR_Staff_Transfer_From_Location'
apiUniqueKey = 'worker_id'
fieldNullException = 'Field_Null_Value'
fieldLengthException = 'Field_Length_Excess'
tarExceptionDataTable = 'ods_hr_api_data_exception_records'
HRStaffTransferFromLocationTableNameDwd = "dwd_hr_staff_transfer_from_location_day_ef"
HRStaffTransferFromLocationTableNameOds = "ods_hr_staff_transfer_from_location_day_ei"
HRStaffTransferFromLocationTableNameOdsDetail = "ods_hr_staff_transfer_from_location_detail_day_ei"

tarEngine = create_engine(
    f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/work_day_stage"
)
batchID = 0
maxCostTime = 0
avgCostTime = 0
createTime = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def syncApiData(user_name, password, query_url, page_index=1, start_time='', end_time=''):
    global avgCostTime, maxCostTime
    credentials = f"{user_name}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    auth = f"Basic {encoded_credentials}"
    if start_time != '':
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
    cost_time = round(time.time() - start, 2)
    avgCostTime += cost_time
    if maxCostTime < cost_time:
        maxCostTime = cost_time
    print(f"page {page_index} data select complete, cost {round(time.time() - start, 2)}s")
    deal_ods(response, query_url, page_index)
    deal_ods_detail(response, query_url, page_index)
    deal_dwd(response)
    print(f"page {page_index} data sync complete, cost {round(time.time() - start, 2)}s")
    if "__next" in response and response["__next"] is not None:
        next_url = response["__next"]
        page_index += 1
        syncApiData(user_name, password, next_url, page_index=page_index)
    else:
        avgCostTime /= page_index


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
    insert_count = tmp_dataframe.to_sql(HRStaffTransferFromLocationTableNameOdsDetail, tarEngine, if_exists='append',
                                        index=False)
    print(f"{HRStaffTransferFromLocationTableNameOdsDetail}数据插入成功，受影响行数：", insert_count)


def deal_ods(response, query_url, page_index):
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods)
    originalParentCount = len(response)
    str_response = json.dumps(response).encode('utf-8')
    compressed_response = zlib.compress(str_response)
    compressed_response_str = base64.b64encode(compressed_response).decode('utf-8')
    tmp_dataframe.loc[len(tmp_dataframe.index)] = [page_index, createTime, compressed_response_str,
                                                   originalParentCount, query_url]
    insert_count = tmp_dataframe.to_sql(HRStaffTransferFromLocationTableNameOds, tarEngine, if_exists='append',
                                        index=False)
    print(f"{HRStaffTransferFromLocationTableNameOds}数据插入成功，受影响行数：", insert_count)


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
        date_obj = datetime.datetime.fromisoformat(date) + datetime.timedelta(hours=8)
        result = date_obj.strftime('%m/%d/%Y %I:%M:%S %p')
    elif len(date) == 4:
        date = f"{date}-01-01T12:00:00.000"
        date_obj = datetime.datetime.fromisoformat(date) + datetime.timedelta(hours=8)
        result = date_obj.strftime('%m/%d/%Y %I:%M:%S %p')
    else:
        print("Exception: unknown date format")
    return result


def format_fields_date(dataframe, index):
    for field_date in FieldsDate:
        dataframe.loc[index, field_date] = deal_date_list(dataframe.loc[index, field_date])


def deal_dwd(response):
    data_frame = pandas.json_normalize(response)
    if data_frame.size == 0:
        print("没有数据")
        return
    data_frame.rename(columns=fieldMapping, inplace=True)
    result_dataframe = data_frame.fillna('')
    null_fields_dataframe = api_field_check(result_dataframe, fieldNullException)
    if null_fields_dataframe.size != 0:
        print("开始插入异常null数据")
        null_except_data_count = null_fields_dataframe.to_sql(tarExceptionDataTable, tarEngine, if_exists='append',
                                                              index=False)
        print(f"{tarExceptionDataTable} null异常数据插入成功，异常数据条数{len(null_fields_dataframe)}，"
              f"插入成功条数{null_except_data_count}")
    for i in range(0, len(result_dataframe.index)):
        format_fields_date(result_dataframe, i)
    if result_dataframe.size == 0:
        print("查询数据集为空")
        return
    result_dataframe = result_dataframe[list(fieldMapping.values())]
    check_repeat_data(result_dataframe)
    try:
        new_column = pandas.Series([createTime] * len(result_dataframe), name='create_date')
        result_dataframe = pandas.concat([result_dataframe, new_column], axis=1)
        insert_count = result_dataframe.to_sql(HRStaffTransferFromLocationTableNameDwd, tarEngine, if_exists='append',
                                               index=False)
    except Exception as e:
        # 判断是否是因为数据超长而造成的失败，如果是则将超长数据记录添加到新的集合
        if '5025' in str(e.__dict__['orig']).split(',')[0]:
            over_length_dataframe = api_field_check(result_dataframe, fieldLengthException)
            if len(over_length_dataframe) != 0:
                print("开始插入异常over length数据")
                except_data_count = over_length_dataframe.to_sql(tarExceptionDataTable, tarEngine, if_exists='append',
                                                                 index=False)
                print(f"{tarExceptionDataTable} over length异常数据插入成功，异常数据条数{len(over_length_dataframe)}，"
                      f"插入成功条数{except_data_count}")
                insert_count = result_dataframe.to_sql(HRStaffTransferFromLocationTableNameDwd, tarEngine,
                                                       if_exists='append',
                                                       index=False)
            else:
                raise e
        else:
            raise e
    print(
        f"{HRStaffTransferFromLocationTableNameDwd}数据插入成功，总数据条数: {len(result_dataframe)}，插入行数：{insert_count}")


def check_repeat_data(dataframe_org):
    worker_id_list = "','".join(dataframe_org['worker_id'].to_list())
    sql = text(
        f"SELECT {','.join(list(fieldMapping.values()))} FROM {HRStaffTransferFromLocationTableNameDwd} WHERE worker_id IN ('{worker_id_list}')")
    src_onn = tarEngine.connect()
    data_frame = pandas.read_sql(sql, src_onn)
    if data_frame.size > 0:
        for index, row in data_frame.iterrows():
            repeat_data = dataframe_org[dataframe_org['worker_id'] == row['worker_id']].iloc[0]
            if row.equals(repeat_data):
                print("数据重复", repeat_data.values)
            else:
                diff_row = row != repeat_data
                print("数据更新，原始数据：\n", row[diff_row], "更新后的数据：\n", repeat_data[diff_row])



def check_length(row):
    over_length_field = []
    for field, value in row.items():
        if isinstance(value, str) and len(value) > 256:
            over_length_field.append(field)
    return over_length_field


def check_null(row):
    null_field_list = []
    for field, value in row.items():
        if field in notNullFieldList and value == '':
            null_field_list.append(field)
    return null_field_list


def api_field_check(dataframe, field_exception_type):
    new_df = pandas.DataFrame(columns=exceptionDataColumn_ods)
    for index, row in dataframe.iterrows():
        if field_exception_type == 'Field_Length_Excess':
            exception_fields = check_length(row)
        else:
            exception_fields = check_null(row)
        if len(exception_fields) != 0:
            unique_code = row[apiUniqueKey]
            new_df.loc[len(new_df.index)] = [unique_code, createTime, apiName, str(row.to_dict()),
                                             ','.join(exception_fields),
                                             field_exception_type]
            if field_exception_type == 'Field_Length_Excess':
                dataframe.drop(index, inplace=True)
    return new_df


if __name__ == '__main__':
    startTimeStr = ''
    endTimeStr = ''
    user = "sb-9e4a42e7-4439-4782-95ce-a149c045c26e!b2390|it-rt-pwc!b39"
    pwd = "9732d1fd-2fb1-4080-97cb-cd82df084219$-BDmkDUlmMek7Dj9bS5w7Tqlzwdm7o2XIi5tPZaGMwQ="
    # transfer_from_location_url = "https://pwc-dev.it-cpi010-rt.cpi.cn40.apps.platform.sapcloud.cn/http/vprofile/transfer_from"
    transfer_from_location_url = "https://pwc.it-cpi010-rt.cpi.cn40.apps.platform.sapcloud.cn/http/vprofile/transfer_from"
    if startTimeStr == '':
        syncApiData(user, pwd, transfer_from_location_url)
    else:
        startTime = datetime.datetime.strptime(startTimeStr, "%Y-%m-%dT%H:%M:%S")
        endTime = datetime.datetime.strptime(endTimeStr, "%Y-%m-%dT%H:%M:%S")
        while startTime < endTime:
            tmpStartTime = startTime
            tmpEndTime = tmpStartTime + datetime.timedelta(days=1) if (tmpStartTime + datetime.timedelta(
                days=1)) < endTime else endTime
            createTime = tmpEndTime
            syncApiData(user, pwd, transfer_from_location_url, start_time=tmpStartTime.strftime("%Y-%m-%dT%H:%M:%S"),
                        end_time=tmpEndTime.strftime("%Y-%m-%dT%H:%M:%S"))
            startTime += datetime.timedelta(days=1)
    print(f"data sync complete, max cost time is {maxCostTime}, avg cost time is {avgCostTime}")
