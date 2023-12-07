# !/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import json
import zlib
import sys
import time
import pandas
import requests
import base64
from sqlalchemy import create_engine
from sqlalchemy import text

resultJobProfilesColumn = ['Job_Code',
                           'Job_Title',
                           'Inactive',
                           'Job_Category_Reference_ID',
                           'Job_Level_Reference_ID',
                           'Job_Level_Reference_Name',
                           'Management_Level_Reference_ID',
                           'Management_Level_Reference_Name'
                           ]

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

tarJobProfilesTableNameDwd = "HRJobProfiles_dwd"
tarJobProfilesTableNameOds = "HRJobProfiles_ods"
tarJobProfilesTableNameOdsDetail = "HRJobProfiles_ods_detail"

tarEngine = create_engine('mysql+pymysql://root@10.158.16.244:9030/WorkDayStage')
batchID = 0
avgCostTime = 0
maxCostTime = 0
createTime = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def getToken(url, user, pwd):
    credentials = f"{user}:{pwd}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    auth = f"Basic {encoded_credentials}"
    headers = {
        "Authorization": auth
    }
    response = requests.get(url, headers=headers, verify=False).json()
    data_frame = pandas.json_normalize(response)
    result = data_frame.get(["token_type", "access_token"])
    return ' '.join(result.values.tolist()[0])


def syncApiData(token, query_url, page_index=1):
    global avgCostTime, maxCostTime
    headers = {
        "Authorization": token
    }
    proxies = requests.utils.getproxies()
    if proxies and 'https' in proxies:
        proxies['https'] = proxies['http']
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
    if "__next" in response["d"] and response["d"]["__next"] is not None:
        next_url = response["d"]["__next"]
        page_index += 1
        syncApiData(token, next_url, page_index=page_index)
    else:
        avgCostTime /= page_index


def deal_ods_detail(response, query_url, page_index=1):
    global batchID
    dataNumber = 0
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods_detail)
    for value in response["d"]["results"]:
        batchID += 1
        dataNumber += 1
        comment = "" if value["externalCode"] is not None else "Exception data， externalCode is None"
        tmp_dataframe.loc[len(tmp_dataframe.index)] = [batchID, createTime, page_index,
                                                       json.dumps(value), dataNumber, query_url, comment]
    insert_count = tmp_dataframe.to_sql(tarJobProfilesTableNameOdsDetail, tarEngine, if_exists='append', index=False)
    print(f"{tarJobProfilesTableNameOdsDetail}数据插入成功，受影响行数：", insert_count)


def deal_ods(response, query_url, page_index):
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods)
    originalParentCount = len(response["d"]["results"])
    str_response = json.dumps(response).encode('utf-8')
    compressed_response = zlib.compress(str_response)
    compressed_response_str = base64.b64encode(compressed_response).decode('utf-8')
    tmp_dataframe.loc[len(tmp_dataframe.index)] = [page_index, createTime, compressed_response_str,
                                                   originalParentCount, query_url]
    insert_count = tmp_dataframe.to_sql(tarJobProfilesTableNameOds, tarEngine, if_exists='append', index=False)
    print(f"{tarJobProfilesTableNameOds}数据插入成功，受影响行数：", insert_count)


def deal_dwd(response):
    # dataframe = pandas.json_normalize(response, record_path=["d", "results"])
    tmp_dataframe = pandas.DataFrame(columns=resultJobProfilesColumn)
    for value in response["d"]["results"]:
        if value.get("externalCode") is None:
            print("the item Job Code is none,", value)
            continue
        Job_Code = value["externalCode"]
        Job_Title = value["name_en_GB"]
        Inactive = value["status"]
        Job_Category_Reference_ID = \
            value["cust_jobcategoryNav"]["label_en_GB"] if value["cust_jobcategoryNav"] is not None else None
        Job_Level_Reference_ID = \
            value["cust_joblevelNav"]["cust_rl"] if value["cust_joblevelNav"] is not None else None
        Job_Level_Reference_Name = \
            value["cust_joblevelNav"]["externalName"] if value["cust_joblevelNav"] is not None else None
        Management_Level_Reference_ID = \
            value["cust_mlNav"]["cust_rid"] if value["cust_mlNav"] is not None else None
        Management_Level_Reference_Name = \
            value["cust_mlNav"]["externalName"] if value["cust_mlNav"] is not None else None

        tmp_dataframe.loc[len(tmp_dataframe.index)] = [Job_Code, Job_Title, Inactive, Job_Category_Reference_ID,
                                                       Job_Level_Reference_ID, Job_Level_Reference_Name,
                                                       Management_Level_Reference_ID, Management_Level_Reference_Name]
    tmp_dataframe = tmp_dataframe.fillna('')
    insert_count = tmp_dataframe.to_sql(tarJobProfilesTableNameDwd, tarEngine, if_exists='append', index=False)
    print(f"{tarJobProfilesTableNameDwd}数据插入成功，受影响行数：", insert_count)


def truncateTable(engine, table_name):
    tarConn = engine.connect()
    tarConn.execute(text(f"truncate table {table_name}"))
    tarConn.close()
    print("table truncate is complete")


if __name__ == '__main__':
    getTokenUrl = "https://pwc-dev.it-cpi010-rt.cpi.cn40.apps.platform.sapcloud.cn/http/vProfile/gettoken"
    getTokenUser = "sb-a8531244-374b-414c-8944-0dbdf941c2e5!b1813|it-rt-pwc-dev!b39"
    getTokenPwd = "f4aee3e5-9539-4568-be98-404a5c6ca253$yxW2FNy_fKA8a1Fjn44SM3zjSt4VGvIbzu9tQnHfWdg="
    requestToken = getToken(getTokenUrl, getTokenUser, getTokenPwd)
    jobProfile_url = "https://api15.sapsf.cn/odata/v2/FOJobCode?$format=json&$expand=cust_mlNav,cust_joblevelNav,cust_jobcategoryNav&$select=externalCode,name_en_GB,status,cust_mlNav/cust_rid,cust_mlNav/externalName,cust_joblevelNav/cust_rl,cust_joblevelNav/externalName,cust_jobcategoryNav/label_en_GB"
    truncateTable(tarEngine, tarJobProfilesTableNameDwd)
    syncApiData(requestToken, jobProfile_url)
    print(f"data sync complete, max cost time is {maxCostTime}, avg cost time is {avgCostTime}")
