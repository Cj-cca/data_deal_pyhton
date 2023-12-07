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
from urllib.parse import quote_plus as urlquote

resultJobProfilesColumn = ['job_code', 'job_title', 'inactive', 'job_category_reference_id', 'job_level_reference_id',
                           'job_level_reference_name', 'management_level_reference_id',
                           'management_level_reference_name']
notNullFieldList = ["job_code", "job_title", "inactive", "management_level_reference_name", "job_level_reference_name",
                    "job_category_reference_id", "job_level_reference_id", "management_level_reference_id"]
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

apiName = 'HR_Job_Profiles'
apiUniqueKey = 'job_code'
fieldNullException = 'Field_Null_Value'
fieldLengthException = 'Field_Length_Excess'
tarExceptionDataTable = 'ods_hr_api_data_exception_records'
tarJobProfilesTableNameDwd = "dwd_hr_job_profiles_day_ef"
tarJobProfilesTableNameOds = "ods_hr_job_profiles_day_ei"
tarJobProfilesTableNameOdsDetail = "ods_hr_job_profiles_detail_day_ei"

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
        if Inactive == "A":
            Inactive = "False"
        elif Inactive == "I":
            Inactive = "True"
        else:
            Inactive = ""

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
    null_fields_dataframe = api_field_check(tmp_dataframe, fieldNullException)
    if null_fields_dataframe.size != 0:
        print("开始插入异常null数据")
        null_except_data_count = null_fields_dataframe.to_sql(tarExceptionDataTable, tarEngine, if_exists='append',
                                                              index=False)
        print(f"{tarExceptionDataTable} null异常数据插入成功，异常数据条数{len(null_fields_dataframe)}，"
              f"插入成功条数{null_except_data_count}")
    try:
        insert_count = tmp_dataframe.to_sql(tarJobProfilesTableNameDwd, tarEngine, if_exists='append', index=False)
    except Exception as e:
        # 判断是否是因为数据超长而造成的失败，如果是则将超长数据记录添加到新的集合
        if '5025' in str(e.__dict__['orig']).split(',')[0]:
            over_length_dataframe = api_field_check(tmp_dataframe, fieldLengthException)
            if len(over_length_dataframe) != 0:
                print("开始插入异常over length数据")
                except_data_count = over_length_dataframe.to_sql(tarExceptionDataTable, tarEngine, if_exists='append',
                                                                 index=False)
                print(f"{tarExceptionDataTable} over length异常数据插入成功，异常数据条数{len(over_length_dataframe)}，"
                      f"插入成功条数{except_data_count}")
                insert_count = tmp_dataframe.to_sql(tarJobProfilesTableNameDwd, tarEngine,
                                                    if_exists='append',
                                                    index=False)
            else:
                raise e
        else:
            raise e
    print(f"{tarJobProfilesTableNameDwd}数据插入成功，受影响行数：", insert_count)


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


def truncateTable(engine, table_name):
    tarConn = engine.connect()
    tarConn.execute(text(f"truncate table {table_name}"))
    tarConn.close()
    print("table truncate is complete")


if __name__ == '__main__':
    getTokenUrl = "https://pwc.it-cpi010-rt.cpi.cn40.apps.platform.sapcloud.cn/http/vprofile/gettoken"
    getTokenUser = "sb-9e4a42e7-4439-4782-95ce-a149c045c26e!b2390|it-rt-pwc!b39"
    getTokenPwd = "9732d1fd-2fb1-4080-97cb-cd82df084219$-BDmkDUlmMek7Dj9bS5w7Tqlzwdm7o2XIi5tPZaGMwQ="
    requestToken = getToken(getTokenUrl, getTokenUser, getTokenPwd)
    # pro api
    jobProfile_url = """https://api15.sapsf.cn/odata/v2/FOJobCode?$format=json&$expand=cust_mlNav,cust_joblevelNav,
    cust_jobcategoryNav&$select=externalCode,name_en_GB,status,cust_mlNav/cust_rid,cust_mlNav/externalName,
    cust_joblevelNav/cust_rl,cust_joblevelNav/externalName,cust_jobcategoryNav/label_en_GB"""
    truncateTable(tarEngine, tarJobProfilesTableNameDwd)
    syncApiData(requestToken, jobProfile_url)
    print(f"data sync complete, max cost time is {maxCostTime}, avg cost time is {avgCostTime}")
