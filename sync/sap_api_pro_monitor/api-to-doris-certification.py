# !/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import json
import re
import zlib
import time
import pandas
import requests
import base64
from sqlalchemy import create_engine
from sqlalchemy import text
from urllib.parse import quote_plus as urlquote

resultCertificationColumn = ['worker_id',
                             'issued_date',
                             'certification_reference_id',
                             'certification_name',
                             'expiration_date',
                             'examination_score',
                             'examination_date', ]

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

fieldMapping = {'d.results.externalCodeNav': 'worker_id',
                'd.results.cust_anothercertificates.results.cust_issueddate': 'issued_date',
                'd.results.cust_anothercertificates.results.cust_certificationNav.externalCode': 'certification_reference_id',
                'd.results.cust_anothercertificates.results.cust_certificationNav.externalName_en_GB': 'certification_name',
                'd.results.cust_anothercertificates.results.cust_expirationdate': 'expiration_date',
                'd.results.cust_anothercertificates.results.cust_scores': 'examination_score',
                'd.results.cust_anothercertificates.results.cust_examinationdate': 'examination_date'}

exceptionDataColumn_ods = ['unique_code', 'create_date', 'api_name', 'exception_data', 'exception_field',
                           'exception_type']

apiName = 'HR_Workers_Certification'
apiUniqueKey = 'worker_id'
fieldNullException = 'Field_Null_Value'
fieldLengthException = 'Field_Length_Excess'
tarExceptionDataTable = 'ods_hr_api_data_exception_records'
tarCertificationTableNameDwd = "dwd_hr_workers_certification_day_ef"
tarCertificationTableNameOds = "ods_hr_workers_certification_day_ei"
tarCertificationTableNameOdsDetail = "ods_hr_workers_certification_detail_day_ei"

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
    deal_certification_dwd(response)
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
    insert_count = tmp_dataframe.to_sql(tarCertificationTableNameOdsDetail, tarEngine, if_exists='append', index=False)
    print(f"{tarCertificationTableNameOdsDetail}数据插入成功，受影响行数：", insert_count)


def deal_ods(response, query_url, page_index):
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods)
    originalParentCount = len(response["d"]["results"])
    str_response = json.dumps(response).encode('utf-8')
    compressed_response = zlib.compress(str_response)
    compressed_response_str = base64.b64encode(compressed_response).decode('utf-8')
    tmp_dataframe.loc[len(tmp_dataframe.index)] = [page_index, createTime, compressed_response_str,
                                                   originalParentCount, query_url]
    insert_count = tmp_dataframe.to_sql(tarCertificationTableNameOds, tarEngine, if_exists='append', index=False)
    print(f"{tarCertificationTableNameOds}数据插入成功，受影响行数：", insert_count)


def deal_certification_dwd(response):
    tmp_dataframe = pandas.DataFrame(columns=resultCertificationColumn)
    for value in response["d"]["results"]:
        if len(value["cust_anothercertificates"]["results"]) == 0:
            print(value["externalCodeNav"]["personKeyNav"]["personIdExternal"],
                  " cust_anothercertificates.results is empty")
            if value["externalCodeNav"] is not None:
                tmp_dataframe.loc[len(tmp_dataframe.index)] = [
                    value["externalCodeNav"]["personKeyNav"]["personIdExternal"], None, None, None, None, None, None]

    data_frame = pandas.json_normalize(response,
                                       record_path=["d", "results", "cust_anothercertificates", "results"],
                                       meta=[["d", "results", "externalCodeNav"]],
                                       record_prefix="d.results.cust_anothercertificates.results.")
    data_frame["d.results.externalCodeNav"] = data_frame["d.results.externalCodeNav"].map(customer_map)
    data_frame.rename(columns=fieldMapping, inplace=True)
    data_frame = data_frame[data_frame["worker_id"].notnull()]
    result_dataframe = data_frame[resultCertificationColumn]
    result_dataframe = pandas.concat([result_dataframe, tmp_dataframe], ignore_index=True)
    for i in range(0, len(result_dataframe.index)):
        result_dataframe.loc[i, "issued_date"] = format_date(result_dataframe.loc[i, "issued_date"])
        result_dataframe.loc[i, "expiration_date"] = format_date(result_dataframe.loc[i, "expiration_date"])
        result_dataframe.loc[i, "examination_date"] = format_date(result_dataframe.loc[i, "examination_date"])

    result_dataframe = result_dataframe.fillna('')
    try:
        insert_count = result_dataframe.to_sql(tarCertificationTableNameDwd, tarEngine, if_exists='append', index=False)
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
                insert_count = result_dataframe.to_sql(tarCertificationTableNameDwd, tarEngine, if_exists='append',
                                                       index=False)
            else:
                raise e
        else:
            raise e
    print(f"{tarCertificationTableNameDwd}数据插入成功，总行数{len(result_dataframe)}，受影响行数：", insert_count)


def customer_map(data):
    return data["personKeyNav"]["personIdExternal"] if data is not None else None


# 将date格式化为：1/1/0001 12:00:00 AM
def format_date(date):
    result = None
    if date is not None:
        match = re.search(r'\d+', date)
        if match:
            dt_str = match.group()
            dt_int = int(dt_str)
            if len(dt_str) == 10:
                dt_int = dt_int
            elif len(dt_str) == 13:
                dt_int = dt_int / 1000
            else:
                return None
            dt = datetime.datetime.fromtimestamp(dt_int) + datetime.timedelta(hours=8)
            result = dt.strftime('%m/%d/%Y %I:%M:%S %p')
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
    certificates_url = "https://api15.sapsf.cn/odata/v2/cust_certificates/?$format=json&$expand=cust_anothercertificates,externalCodeNav/personKeyNav,cust_anothercertificates/cust_certificationNav&$select=externalCodeNav/personKeyNav/personIdExternal,cust_anothercertificates/cust_certificationNav/externalName_en_GB,cust_anothercertificates/cust_certificationNav/externalCode,cust_anothercertificates/cust_examinationdate,cust_anothercertificates/cust_scores,cust_anothercertificates/cust_expirationdate,cust_anothercertificates/cust_issueddate&$filter=externalCodeNav/status eq 'active'"
    truncateTable(tarEngine, tarCertificationTableNameDwd)
    syncApiData(requestToken, certificates_url)
    print(f"data sync complete, max cost time is {maxCostTime}, avg cost time is {avgCostTime}")
