# !/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import time
import json
import zlib
import pandas
import requests
import base64
from sqlalchemy import create_engine
from sqlalchemy import text
from urllib.parse import quote_plus as urlquote

notNullField = []
fieldMapping = {'Data.BKG_ID': 'bkg_id',
                'Data.BKG_START': 'bkg_start',
                'Data.BKG_END': 'bkg_end',
                'Data.BKG_TIME': 'bkg_time',
                'Data.BKG_JOB_ID': 'bkg_job_id',
                'Data.BKG_JOB_ID_DESCR': 'bkg_job_id_descr',
                'Data.BKGJobCode': 'bkg_job_code',
                'Data.BKGEmployeeAltID': 'bkg_employee_alt_id',
                'Data.BKGLocalEmployeeAltID': 'bkg_local_employee_alt_id',
                'Data.BKGClientCode': 'bkg_client_code',
                'Data.BKGClientName': 'bkg_client_name',
                'Data.BKG_LOADING': 'bkg_loading',
                'Data.BKG_RES_ID': 'bkg_res_id',
                'Data.BKG_STATUS': 'bkg_status',
                'Data.BKG_DELETED': 'bkg_deleted',
                'Data.BKGLocalEmployeeID': 'bkg_local_employee_id'}

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

tarExceptionDataTable = ''
tarTableNameDwd = "ods_tbl_talent_link_bookings_day_ei"
tarTableNameOds = "ods_tbl_talent_link_bookings_day_ei"
tarTableNameOdsDetail = ""

tarEngine = create_engine(
    f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.15.148:6030/talenlink"
)
# tarEngine = create_engine(
#     f"mysql+pymysql://root@10.158.16.244:9030/work_day_stage"
# )
batchID = 0
avgCostTime = 0
maxCostTime = 0
createTime = datetime.datetime.now().date()


def syncApiData(query_url, page_index=1):
    global avgCostTime, maxCostTime
    proxies = requests.utils.getproxies()
    if proxies and 'https' in proxies:
        proxies['https'] = proxies['http']
    print(f"======start sync data from page {query_url}======")
    start = time.time()
    response = requests.get(query_url, proxies=proxies, verify=False)
    if response.status_code == 200:
        data = response.json()
        cost_time = round(time.time() - start, 2)
        avgCostTime += cost_time
        if maxCostTime < cost_time:
            maxCostTime = cost_time
        print(f"page {page_index} data select complete, cost {round(time.time() - start, 2)}s")
        # deal_ods(response, query_url, page_index)
        # deal_ods_detail(response, query_url, page_index)
        deal_dwd(data)
        print(f"======page {page_index} data sync complete, cost {round(time.time() - start, 2)}s======")
    else:
        print("请求失败，状态码：", response.status_code)
        if page_index < 4:
            syncApiData(query_url, page_index + 1)


def deal_ods_detail(response, query_url, page_index=1):
    global batchID
    data_number = 0
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods_detail)
    for value in response["result"]:
        batchID += 1
        data_number += 1
        comment = "" if value[f"{notNullField}"] is not None else "Exception data， externalCode is None"
        tmp_dataframe.loc[len(tmp_dataframe.index)] = [batchID, createTime, page_index,
                                                       json.dumps(value), data_number, query_url, comment]
    insert_count = tmp_dataframe.to_sql(tarTableNameOdsDetail, tarEngine, if_exists='append', index=False)
    print(f"{tarTableNameOdsDetail}数据插入成功，受影响行数：", insert_count)


def deal_ods(response, query_url, page_index):
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods)
    original_parent_count = len(response["result"])
    str_response = json.dumps(response).encode('utf-8')
    compressed_response = zlib.compress(str_response)
    compressed_response_str = base64.b64encode(compressed_response).decode('utf-8')
    tmp_dataframe.loc[len(tmp_dataframe.index)] = [page_index, createTime, compressed_response_str,
                                                   original_parent_count, query_url]
    insert_count = tmp_dataframe.to_sql(tarTableNameOds, tarEngine, if_exists='append', index=False)
    print(f"{tarTableNameOds}数据插入成功，受影响行数：", insert_count)


def deal_dwd(response):
    data_frame = pandas.json_normalize(response["result"], record_path=["values"])
    if data_frame.size == 0:
        print("没有数据")
        return
    data_frame.rename(columns=fieldMapping, inplace=True)
    data_frame["etl_date"] = createTime
    result_dataframe = data_frame.fillna('')
    insert_count = -1
    try:
        insert_count = result_dataframe.to_sql(tarTableNameOds, tarEngine, if_exists='append', index=False)
    except Exception as e:
        # 判断是否是因为数据超长而造成的失败，如果是则将超长数据记录添加到新的集合
        print(e)
    print(f"{tarTableNameOds}数据插入成功，总数据条数: {len(result_dataframe)}，插入行数：{insert_count}")


def truncate_table(table_name):
    tar_conn = tarEngine.connect()
    tar_conn.execute(text(f"truncate table {table_name}"))
    tar_conn.close()
    print("table truncate is complete")


# 2024-04-20，2024-04-24
# '2019-10-27'
if __name__ == '__main__':
    apiType = "bookings"
    startTimeStr = ''
    endTimeStr = '2024-05-28'
    date_gap = 1
    # truncate_table(tarTableNameOds)
    # 增量同步数据
    APIDeltaUrl = "https://cncapppwv5008.asia.pwcinternal.com/talentlinkapi/v2/{apiType}/delta/{apiRequestStartTime}/{apiRequestEndTime}"
    if startTimeStr == '':
        apiRequestStartTime = datetime.date.today() - datetime.timedelta(days=1)
        apiRequestEndTime = datetime.date.today()
        URL = APIDeltaUrl.format(apiType=apiType, apiRequestStartTime=apiRequestStartTime,
                                 apiRequestEndTime=apiRequestEndTime)
        syncApiData(URL)
    else:
        startTime = datetime.datetime.strptime(startTimeStr, "%Y-%m-%d").date()
        endTime = datetime.datetime.strptime(endTimeStr, "%Y-%m-%d").date()
        while startTime < endTime:
            tmpStartTime = startTime
            tmpEndTime = tmpStartTime + datetime.timedelta(days=date_gap) if (tmpStartTime + datetime.timedelta(
                days=date_gap)) < endTime else endTime
            createTime = tmpEndTime
            URL = APIDeltaUrl.format(apiType=apiType, apiRequestStartTime=tmpStartTime,
                                     apiRequestEndTime=tmpEndTime)
            print("当前批次数据同步，开始时间：", tmpStartTime, "，结束时间：", tmpEndTime)
            syncApiData(URL)
            startTime += datetime.timedelta(days=date_gap)
    print(f"data sync complete, max cost time is {maxCostTime}, avg cost time is {avgCostTime}")
