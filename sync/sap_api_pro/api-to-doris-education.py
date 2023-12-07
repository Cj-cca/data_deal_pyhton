# !/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import json
import zlib
import time
import pandas
import requests
import base64
from sqlalchemy import create_engine
from sqlalchemy import text
from urllib.parse import quote_plus as urlquote

resultEducationColumn = ['worker_id',
                         'first_year_attended',
                         'degree',
                         'is_highest_level_of_education',
                         'last_year_attended',
                         'school_id',
                         'school_name',
                         'education_country',
                         'degree_receiving_date',
                         'field_of_study']

resultColumn_ods = ['page_index',
                    'create_time',
                    'response_data',
                    'response_object_count',
                    'query_url',
                    ]

resultColumn_ods_detail = ['batch_id',
                           'create_time',
                           'current_page_num',
                           'response_data',
                           'response_object_num',
                           'query_url',
                           'comment'
                           ]

tarEducationTableNameDwd = "dwd_hr_workers_education_day_ef"
tarEducationTableNameOds = "ods_hr_workers_education_day_ei"
tarEducationTableNameOdsDetail = "ods_hr_workers_education_detail_day_ei"

tarEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/work_day_stage"
    )
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
    deal_education_dwd(response)
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
        comment = "" if value["externalCodeNav"] is not None else "Exception data， externalCodeNav is None"
        tmp_dataframe.loc[len(tmp_dataframe.index)] = [batchID, createTime, page_index,
                                                       json.dumps(value), dataNumber, query_url, comment]
    insert_count = tmp_dataframe.to_sql(tarEducationTableNameOdsDetail, tarEngine, if_exists='append', index=False)
    print(f"{tarEducationTableNameOdsDetail}数据插入成功，总条数{len(tmp_dataframe)}，受影响行数：", insert_count)


def deal_ods(response, query_url, page_index):
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods)
    originalParentCount = len(response["d"]["results"])
    str_response = json.dumps(response).encode('utf-8')
    compressed_response = zlib.compress(str_response)
    compressed_response_str = base64.b64encode(compressed_response).decode('utf-8')
    tmp_dataframe.loc[len(tmp_dataframe.index)] = [page_index, createTime, compressed_response_str,
                                                   originalParentCount, query_url]
    insert_count = tmp_dataframe.to_sql(tarEducationTableNameOds, tarEngine, if_exists='append', index=False)
    print(f"{tarEducationTableNameOds}数据插入成功，总条数：{len(tmp_dataframe)}，受影响行数：", insert_count)


def deal_education_dwd(response):
    tmp_dataframe = pandas.DataFrame(columns=resultEducationColumn)
    for value in response["d"]["results"]:
        if value.get("externalCodeNav") is None:
            print("the item work id is none,", value)
            continue
        Worker_ID = value["externalCodeNav"]["personKeyNav"]["personIdExternal"]
        if len(value["cust_anothereducation"]["results"]) == 0:
            print(value["externalCodeNav"]["personKeyNav"]["personIdExternal"],
                  " cust_anothereducation.results is empty")
            tmp_dataframe.loc[len(tmp_dataframe.index)] = [Worker_ID, None, None, None, None, None, None, None, None,
                                                           None]
        else:
            for value_sub in value["cust_anothereducation"]["results"]:
                First_Year_Attended = value_sub["cust_yearofadmission"]
                Degree = value_sub["cust_certificatediplomadegreeNav"]["results"][0]["label_en_GB"]
                Is_Highest_Level_of_Education = value_sub["cust_isityourhighesteducation"]
                Last_Year_Attended = value_sub["cust_yearofcompletion"]
                School_ID = value_sub["cust_school"]
                School_Name = value_sub["cust_schoolNav"]["results"][0]["label_en_GB"]
                Education_Country = value_sub["cust_schoolcountryterritoryNav"]["results"][0]["label_en_GB"]
                Degree_Receiving_Date = value_sub["cust_certificatediplomadegreereceived"]
                Field_Of_Study = value_sub["cust_fieldofstudyNav"]["results"][0]["label_en_GB"]
                tmp_dataframe.loc[len(tmp_dataframe.index)] = [Worker_ID, First_Year_Attended, Degree,
                                                               Is_Highest_Level_of_Education, Last_Year_Attended,
                                                               School_ID, School_Name, Education_Country,
                                                               Degree_Receiving_Date, Field_Of_Study]
    tmp_dataframe = tmp_dataframe.fillna('')
    insert_count = tmp_dataframe.to_sql(tarEducationTableNameDwd, tarEngine, if_exists='append', index=False)
    print(f"{tarEducationTableNameDwd}数据插入成功，总条数：{len(tmp_dataframe)}，受影响行数：", insert_count)


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
    education_url = "https://api15.sapsf.cn/odata/v2/cust_education?$format=json&$expand=externalCodeNav/personKeyNav,cust_anothereducation,cust_anothereducation/cust_schoolcountryterritoryNav,cust_anothereducation/cust_fieldofstudyNav,cust_anothereducation/cust_certificatediplomadegreeNav,cust_anothereducation/cust_schoolNav&$select=externalCodeNav/personKeyNav/personIdExternal,cust_anothereducation/cust_school,cust_anothereducation/cust_schoolNav/label_en_GB,cust_anothereducation/cust_fieldofstudyNav/label_en_GB,cust_anothereducation/cust_yearofadmission,cust_anothereducation/cust_yearofcompletion,cust_anothereducation/cust_isityourhighesteducation,cust_anothereducation/cust_schoolcountryterritoryNav/label_en_GB,cust_anothereducation/cust_certificatediplomadegreeNav/label_en_GB,cust_anothereducation/cust_certificatediplomadegreereceived"
    truncateTable(tarEngine, tarEducationTableNameDwd)
    syncApiData(requestToken, education_url)
    print(f"data sync complete, max cost time is {maxCostTime}, avg cost time is {avgCostTime}")
