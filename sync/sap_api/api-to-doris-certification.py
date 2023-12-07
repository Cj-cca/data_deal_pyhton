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

resultCertificationColumn = ['Worker_ID',
                             'Issued_Date',
                             'Certification_Reference_ID',
                             'Certification_Name',
                             'Expiration_Date',
                             'Examination_Score',
                             'Examination_Date', ]

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

fieldMapping = {'d.results.externalCodeNav': 'Worker_ID',
                'd.results.cust_anothercertificates.results.cust_issueddate': 'Issued_Date',
                'd.results.cust_anothercertificates.results.cust_certificationNav.externalCode': 'Certification_Reference_ID',
                'd.results.cust_anothercertificates.results.cust_certificationNav.externalName_en_GB': 'Certification_Name',
                'd.results.cust_anothercertificates.results.cust_expirationdate': 'Expiration_Date',
                'd.results.cust_anothercertificates.results.cust_scores': 'Examination_Score',
                'd.results.cust_anothercertificates.results.cust_examinationdate': 'Examination_Date'}

tarCertificationTableNameDwd = "HRWorkers_Certification_dwd"
tarCertificationTableNameOds = "HRWorkers_Certification_ods"
tarCertificationTableNameOdsDetail = "HRWorkers_Certification_ods_detail"

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
    data_frame = data_frame[data_frame["Worker_ID"].notnull()]
    result_dataframe = data_frame[resultCertificationColumn]
    result_dataframe = pandas.concat([result_dataframe, tmp_dataframe], ignore_index=True)
    for i in range(0, len(result_dataframe.index)):
        result_dataframe.loc[i, "Issued_Date"] = format_date(result_dataframe.loc[i, "Issued_Date"])
        result_dataframe.loc[i, "Expiration_Date"] = format_date(result_dataframe.loc[i, "Expiration_Date"])
        result_dataframe.loc[i, "Examination_Date"] = format_date(result_dataframe.loc[i, "Examination_Date"])

    result_dataframe = result_dataframe.fillna('')
    insert_count = result_dataframe.to_sql(tarCertificationTableNameDwd, tarEngine, if_exists='append', index=False)
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
            dt = datetime.datetime.fromtimestamp(dt_int)
            result = dt.strftime('%m/%d/%Y %I:%M:%S %p')
    return result


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
    certificates_url = "https://api15.sapsf.cn/odata/v2/cust_certificates/?$format=json&$expand=cust_anothercertificates,externalCodeNav/personKeyNav,cust_anothercertificates/cust_certificationNav&$select=externalCodeNav/personKeyNav/personIdExternal,cust_anothercertificates/cust_certificationNav/externalName_en_GB,cust_anothercertificates/cust_certificationNav/externalCode,cust_anothercertificates/cust_examinationdate,cust_anothercertificates/cust_scores,cust_anothercertificates/cust_expirationdate,cust_anothercertificates/cust_issueddate"
    truncateTable(tarEngine, tarCertificationTableNameDwd)
    syncApiData(requestToken, certificates_url)
    print(f"data sync complete, max cost time is {maxCostTime}, avg cost time is {avgCostTime}")
