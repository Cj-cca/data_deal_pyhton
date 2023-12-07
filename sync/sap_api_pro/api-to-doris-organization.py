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

resultOrganizationColumn = ['reference_id', 'organization_name', 'organization_code', 'subtype', 'type',
                            'top_level_organization', 'superior_organization', 'top_level_organization_id',
                            'superior_organization_id']

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

tarOrganizationTableNameDwd = "dwd_hr_organization_day_ef"
tarOrganizationTableNameOds = "ods_hr_organization_day_ei"
tarOrganizationTableNameOdsDetail = "ods_hr_organization_detail_day_ei"

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
    insert_count = tmp_dataframe.to_sql(tarOrganizationTableNameOdsDetail, tarEngine, if_exists='append', index=False)
    print(f"{tarOrganizationTableNameOdsDetail}数据插入成功，总行数：{len(tmp_dataframe)}，受影响行数：", insert_count)


def deal_ods(response, query_url, page_index):
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods)
    originalParentCount = len(response["d"]["results"])
    str_response = json.dumps(response).encode('utf-8')
    compressed_response = zlib.compress(str_response)
    compressed_response_str = base64.b64encode(compressed_response).decode('utf-8')
    tmp_dataframe.loc[len(tmp_dataframe.index)] = [page_index, createTime, compressed_response_str,
                                                   originalParentCount, query_url]
    insert_count = tmp_dataframe.to_sql(tarOrganizationTableNameOds, tarEngine, if_exists='append', index=False)
    print(f"{tarOrganizationTableNameOds}数据插入成功，总行数：{len(tmp_dataframe)}，受影响行数：", insert_count)


def deal_dwd(response):
    result_dataframe = pandas.DataFrame(columns=resultOrganizationColumn)
    for value in response["d"]["results"]:
        if value.get("cust_l5Nav") is None:
            print("the item Job Code is none,", value)
            continue
        if value.get("cust_l5Nav").get("cust_rid") is None:
            print("the item Job Code is none,", value)
            continue
        Reference_ID = value.get("cust_l5Nav").get("cust_rid")
        Organization_Code = value["externalCode"]
        OrganizationName = value["name_en_GB"]
        Superior_Organization = value["cust_l4"]
        Superior_Organization_ID = \
            value["cust_l4Nav"]["cust_rid"] if value["cust_l4Nav"] is not None else None
        result_dataframe.loc[len(result_dataframe.index)] = [Reference_ID, OrganizationName, Organization_Code,
                                                             "Cost Center Hierarchy", "Cost Center Hierarchy",
                                                             "CHN TOP", Superior_Organization, "W1_HCM_CCH_CHN_TOP",
                                                             Superior_Organization_ID]
    result_dataframe = result_dataframe.fillna('')
    insert_count = result_dataframe.to_sql(tarOrganizationTableNameDwd, tarEngine, if_exists='append', index=False)
    print(f"{tarOrganizationTableNameDwd}数据插入成功，总行数：{len(result_dataframe)}，受影响行数：", insert_count)


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
    organization_url = "https://api15.sapsf.cn/odata/v2/FOCostCenter?$format=json&$expand=cust_l5Nav,cust_l4Nav&$select=externalCode,name_en_GB,cust_l5Nav/cust_rid,cust_l5Nav/externalName,cust_l5,cust_l4Nav/cust_rid,cust_l4Nav/externalName,cust_l4"
    truncateTable(tarEngine, tarOrganizationTableNameDwd)
    syncApiData(requestToken, organization_url)
    print(f"data sync complete, max cost time is {maxCostTime}, avg cost time is {avgCostTime}")
