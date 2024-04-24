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

selectColumn = ["Department", "ProjectNature", "Territory", "BudgetNature", "BudgetFY", "BudgetCategory", "AccountCode",
                "AccCodeDescription", "SubCategoryCode", "SubCategory", "Item", "SubItem", "Currency", "AnnualBudget",
                "AdjustBudgetAmount", "IsDeleted", "Id", "LastModificationTime", "IsComplete", "IsDisable"]

fieldMapping = {"Department": "department", "ProjectNature": "project_nature", "Territory": "territory",
                "BudgetNature": "budget_nature", "BudgetFY": "budget_fy", "BudgetCategory": "budget_category",
                "AccountCode": "account_code", "AccCodeDescription": "acc_code_description",
                "SubCategoryCode": "sub_category_code", "SubCategory": "sub_category", "Item": "item",
                "SubItem": "sub_item", "Currency": "currency", "AnnualBudget": "annual_budget",
                "AdjustBudgetAmount": "adjust_budget_amount", "IsDeleted": "is_deleted", "Id": "id",
                "LastModificationTime": "last_modification_time", "IsComplete": "is_complete",
                "IsDisable": "is_disable"}
settingsCode = "BudgetFile"
tarTableNameOds = "ods_fin_budget_file_hour_ei"

avgCostTime = 0
maxCostTime = 0
totalDataCount = 0
totalRequestDataCount = 0
totalInsertCount = 0
createTime = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
delta = datetime.timedelta(days=1)
search_start = (createTime - delta).strftime("%Y-%m-%dT00:00:00")
search_end = createTime.strftime("%Y-%m-%dT00:00:00")
tarEngine = create_engine(
    f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.15.148:6030/procurement_all"
)
# tarEngine = create_engine(
#     f"mysql+pymysql://root:@10.158.16.244:9030/procurement_all"
# )


def getToken(url, app_code, secret_key):
    body = {
        "appCode": app_code,
        "secretKey": secret_key
    }
    response = requests.post(url, json=body, verify=False)
    if response.status_code == 200:
        # Request was successful
        print("POST request successful!")
        data_frame = pandas.json_normalize(response.json())
        return data_frame.get("data.token")[0]
    else:
        # Request failed
        print("POST request failed!")
        print("Status code:", response.status_code)
        print("Error message:", response.text)


def syncApiData(token, query_url, page_index=1, search_after=None, filters=None):
    global avgCostTime, maxCostTime, totalDataCount, totalRequestDataCount
    headers = {
        "Authorization": "Bearer " + str(token)
    }
    body = {
        "SearchAfter": [] if search_after is None else search_after,
        "settingsCode": settingsCode,
        "keyword": "",
        "filters": [] if filters is None else filters,
        "pageSize": 1000,
        "sorts": [
            {
                "field": "LastModificationTime",
                "isAscending": True
            },
            {
                "field": "Id",
                "isAscending": True
            }
        ]
    }

    proxies = requests.utils.getproxies()
    if proxies and 'https' in proxies:
        proxies['https'] = proxies['http']
    start = time.time()
    response = requests.post(query_url, headers=headers, json=body, proxies=proxies, verify=False).json()
    cost_time = round(time.time() - start, 2)
    print(f"page {page_index} data select complete, cost {cost_time}s")
    if len(response['data']['data']) == 0:
        avgCostTime /= page_index
        print("该批次数据为空，数据查询完成")
        return
    avgCostTime += cost_time
    if maxCostTime < cost_time:
        maxCostTime = cost_time
    totalDataCount = response['data']['totalCount']
    data_frame = pandas.json_normalize(response, record_path=["data", "data"])
    last_modification_time = data_frame.loc[len(data_frame) - 1]['LastModificationTime']
    ids = data_frame.loc[len(data_frame) - 1]['Id']
    totalRequestDataCount += len(data_frame)
    deal_dwd(data_frame)
    search_after = [str(last_modification_time), str(ids)]
    page_index += 1
    syncApiData(token, query_url, page_index=page_index, search_after=search_after)


def deal_dwd(data_frame):
    global totalInsertCount
    result_dataframe = data_frame[selectColumn]
    result_dataframe.rename(columns=fieldMapping, inplace=True)
    new_column = pandas.Series([createTime] * len(result_dataframe), name='upload_time')
    result_dataframe = pandas.concat([result_dataframe, new_column], axis=1)
    insert_count = result_dataframe.to_sql(tarTableNameOds, tarEngine, if_exists='append', index=False)
    if insert_count is None:
        insert_count = 0
        print("insert_count is None")
    totalInsertCount += insert_count
    print(f"{tarTableNameOds}数据插入成功，总行数{len(result_dataframe)}，受影响行数：", insert_count)


if __name__ == '__main__':
    getTokenUrl = "https://search.asia.pwcinternal.com/api/auth/token"
    # pro
    appCode = "DataWarehouse"
    secretKey = "7631295a14c044a2a52e193b880f400f"

    # uat
    # appCode = "DataWarehouse"
    # secretKey = "6a569ee03a724c748d62c910c4a3ffe4"
    requestToken = getToken(getTokenUrl, appCode, secretKey)
    data_url = "https://search.asia.pwcinternal.com/api/open-api/search"
    filter1 = {
        "field": "LastModificationTime",
        "operator": "gte",
        "values": [
            search_start
        ]
    }
    filter2 = {
        "field": "LastModificationTime",
        "operator": "lte",
        "values": [
            search_end
        ]
    }
    filter_all = [filter1, filter2]
    # syncApiData(requestToken, data_url, filters=filter_all)
    syncApiData(requestToken, data_url)
    print(
        f"数据同步完成,数据总条数：{totalDataCount},请求到的数据条数{totalRequestDataCount},实际插入数据条数：{totalInsertCount},请求数据最长时间：{maxCostTime},平均请求时间：{avgCostTime}")
    totalInsertCount = 0
