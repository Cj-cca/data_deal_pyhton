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

exceptionDataColumn_ods = ['unique_code', 'create_date', 'api_name', 'exception_data', 'exception_field',
                           'exception_type']

apiName = 'HR_Workers_Education'
apiUniqueKey = 'worker_id'
fieldNullException = 'Field_Null_Value'
fieldLengthException = 'Field_Length_Excess'
tarExceptionDataTable = 'ods_hr_api_data_exception_records'
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
                Degree = None if len(value_sub["cust_certificatediplomadegreeNav"]["results"]) == 0 else \
                    value_sub["cust_certificatediplomadegreeNav"]["results"][0]["label_en_GB"]
                Is_Highest_Level_of_Education = value_sub["cust_isityourhighesteducation"]
                if Is_Highest_Level_of_Education == "Y":
                    Is_Highest_Level_of_Education = "True"
                elif Is_Highest_Level_of_Education == "N":
                    Is_Highest_Level_of_Education = "False"
                else:
                    Is_Highest_Level_of_Education = ""
                Last_Year_Attended = value_sub["cust_yearofcompletion"]
                School_ID = value_sub["cust_school"]
                School_Name = None if len(value_sub["cust_schoolNav"]["results"]) == 0 else \
                    value_sub["cust_schoolNav"]["results"][0]["label_en_GB"]
                Education_Country = handle_education_country(value_sub["cust_schoolcountryterritoryNav"]["results"])
                Degree_Receiving_Date = value_sub["cust_certificatediplomadegreereceived"]
                Field_Of_Study = None if len(value_sub["cust_fieldofstudyNav"]["results"]) == 0 else \
                    value_sub["cust_fieldofstudyNav"]["results"][0]["label_en_GB"]
                tmp_dataframe.loc[len(tmp_dataframe.index)] = [Worker_ID, First_Year_Attended, Degree,
                                                               Is_Highest_Level_of_Education, Last_Year_Attended,
                                                               School_ID, School_Name, Education_Country,
                                                               Degree_Receiving_Date, Field_Of_Study]
    for i in range(0, len(tmp_dataframe.index)):
        tmp_dataframe.loc[i, "first_year_attended"] = format_date(tmp_dataframe.loc[i, "first_year_attended"])
        tmp_dataframe.loc[i, "last_year_attended"] = format_date(tmp_dataframe.loc[i, "last_year_attended"])
    tmp_dataframe = tmp_dataframe.fillna('')
    try:
        insert_count = tmp_dataframe.to_sql(tarEducationTableNameDwd, tarEngine, if_exists='append', index=False)
    except Exception as e:
        # 判断是否是因为数据超长而造成的失败，如果是则将超长数据记录添加到新的集合
        if '5025' in str(e.__dict__['orig']).split(',')[0]:
            over_length_dataframe = api_field_check(tmp_dataframe, fieldLengthException)
            if len(over_length_dataframe) != 0:
                print("开始插入异常over length数据")
                except_data_count = over_length_dataframe.to_sql(tarExceptionDataTable, tarEngine, if_exists='append',
                                                                 index=False)
                print(f"{tarExceptionDataTable}over length异常数据插入成功，异常数据条数{len(over_length_dataframe)}，"
                      f"插入成功条数{except_data_count}")
                insert_count = tmp_dataframe.to_sql(tarEducationTableNameDwd, tarEngine, if_exists='append',
                                                    index=False)
            else:
                raise e
        else:
            raise e
    print(f"{tarEducationTableNameDwd}数据插入成功，总条数：{len(tmp_dataframe)}，受影响行数：", insert_count)


def handle_education_country(value):
    country = None if len(value) == 0 else \
        value[0]["label_en_GB"]
    if country is not None:
        if country == "Hong Kong, China":
            country = 'Hong Kong'
        elif country == "Macao, China":
            country = 'Macao'
        elif country == "Taiwan, China":
            country = 'Taiwan'
    return country


def format_date(date):
    result = ''
    if date == '' or date == 'null' or date is None:
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


def check_length(row):
    over_length_field = []
    if len(row['worker_id']) > 256:
        over_length_field.append(row['worker_id'])
    return over_length_field


def api_field_check(dataframe, field_exception_type):
    new_df = pandas.DataFrame(columns=exceptionDataColumn_ods)
    exception_fields = []
    for index, row in dataframe.iterrows():
        if field_exception_type == 'Field_Length_Excess':
            exception_fields = check_length(row)
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
    education_url = "https://api15.sapsf.cn/odata/v2/cust_education?$format=json&$expand=externalCodeNav/personKeyNav,cust_anothereducation,cust_anothereducation/cust_schoolcountryterritoryNav,cust_anothereducation/cust_fieldofstudyNav,cust_anothereducation/cust_certificatediplomadegreeNav,cust_anothereducation/cust_schoolNav&$select=externalCodeNav/personKeyNav/personIdExternal,cust_anothereducation/cust_school,cust_anothereducation/cust_schoolNav/label_en_GB,cust_anothereducation/cust_fieldofstudyNav/label_en_GB,cust_anothereducation/cust_yearofadmission,cust_anothereducation/cust_yearofcompletion,cust_anothereducation/cust_isityourhighesteducation,cust_anothereducation/cust_schoolcountryterritoryNav/label_en_GB,cust_anothereducation/cust_certificatediplomadegreeNav/label_en_GB,cust_anothereducation/cust_certificatediplomadegreereceived&$filter=externalCodeNav/status eq 'active'"
    truncateTable(tarEngine, tarEducationTableNameDwd)
    syncApiData(requestToken, education_url)
    print(f"data sync complete, max cost time is {maxCostTime}, avg cost time is {avgCostTime}")
