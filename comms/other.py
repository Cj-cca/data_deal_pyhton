# !/usr/bin/env python
# -*- coding: utf-8 -*-
import time

import pymysql
import re
import requests
import base64
import pandas as pd
from tqdm import tqdm
import numpy as np

resultJobProfilesColumn = ['Worker_ID', 'Effective_Date', 'Location_Name']


def deal_json_keys(response):
    original_map = {}
    for filed in resultJobProfilesColumn:
        original_map[filed.lower()] = filed
    result_map = {}
    for key in response[0]:
        if key.lower() in original_map:
            result_map[key] = original_map[key.lower()]
        else:
            print(key)

    print(f"org：{len(original_map)},tar:{len(result_map)}")
    print(result_map)
    return result_map


def convert_to_snake_case(map_data):
    for key in map_data:
        v = map_data[key]
        if v.find("_"):
            value = '_'.join(word.lower() for word in v.split("_"))
        else:
            '_'.join(word.lower() for word in re.findall('[A-Za-z][a-z]*', v))

        map_data[key] = value
    return map_data


def convert_to_snake_case_new(string):
    tmp = re.sub(r'([A-Z]{2,})', lambda x: x.group().lower().title(), string)
    words = re.findall('[A-Za-z0-9][a-z]*', tmp)
    result = '_'.join(word.lower() for word in words)
    return result


def syncApiData(user_name, password, query_url, page_index=1):
    credentials = f"{user_name}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    auth = f"Basic {encoded_credentials}"
    headers = {
        "Authorization": auth
    }
    proxies = requests.utils.getproxies()
    if proxies and 'https' in proxies:
        proxies['https'] = proxies['http']
    print(f"start sync data from page {page_index}")
    response = requests.get(query_url, headers=headers, proxies=proxies, verify=False).json()
    map1 = deal_json_keys(response)


def match_str(tar_str, fields):
    for field in fields:
        str_new = tar_str.replace("_", "")
        field_new = field.replace("_", "")
        if str_new.lower() in field_new.lower():
            print(field)


def sql_create(src_fields, tar_fields, database, table_name):
    tar_fields_new = {}
    select_fields = []
    for field in tar_fields:
        tar_fields_new[field.replace("_", "").lower()] = field
    for field in src_fields:
        if field.replace("_", "").lower() in tar_fields_new:
            select_fields.append(f"{field} as {tar_fields_new[field.replace('_', '').lower()]}")
        else:
            print(f"======{field}======")
    tmp_select_fields = ',\n'.join(select_fields)
    sql = f"select {tmp_select_fields} \nfrom {database}.{table_name}"
    print(sql)


def check_length(row):
    for field, value in row.items():
        if isinstance(value, str) and len(value) > 8:
            return field
    return ''


def change_dataframe(df1):
    data1 = {'Name': ['Alice', 'Bob', 'Charlie', 'David'],
             'Age': [25, 30, 105, 28],  # 增加一个长度超过100的值
             'City': ['New York', 'Los Angeles', 'Chicago', 'San Francisco']}

    df_tmp = pd.DataFrame(data1)
    print(f"({','.join(df1['Name'].to_list())})")
    new_df = pd.DataFrame(columns=df1.columns)
    for index, row in df1.iterrows():
        find = df_tmp[df_tmp['Name'] == row['Name']].iloc[0]
        if check_length(row) != '':
            print(row['Name'])
            print(str(row.to_dict()))
            df1.drop(index, inplace=True)
    return new_df


# 使用tqdm来对程序的进度展示
def show_run_process():
    for i in tqdm(range(1, 100)):
        time.sleep(0.1)


# def handle_df(series):
#     for value in handleValue:
#         v = series[value]
#         series[value] = v * 5 if v > 5 else v
#     return series


if __name__ == '__main__':
    # srcFields = ["intCustomFieldValueID",
    #              "chRegionCode",
    #              "intCustomFieldID",
    #              "nvcCode",
    #              "nvcValue",
    #              "intSortOrder",
    #              "btIsDefaultValue",
    #              "daTerminationDate"]
    # tarFields = ["int_custom_field_value_id",
    #              "ch_region_code",
    #              "int_custom_field_id",
    #              "nvc_code",
    #              "nvc_value",
    #              "int_sort_order",
    #              "bt_is_default_value",
    #              "da_termination_date"]
    # databases = 'PwCMDM.Core'
    # tableName = 'tblCustomFieldValue'
    # sql_create(srcFields, tarFields, databases, tableName)

    notNullFieldList = """intAssgTimeDtlID
,intAssgnmtID
,chClientCode
,chJobCode
,chAsgOfficeCode
,chAsgGroupCode
,sdPerEndDate
,sdPerEntryDate
,sdProcessDate
,chProjectCode
,chYear
,chTimeJnlType
,sdDailyDate
,sintLineNo
,chOrigOfficeCode
,chVoucherNo
,chJnlNo
,dcHours
,dcTargetRate
,dcWipTargetAmt
,dcBudgetRate
,dcWIPBudgetAmt
,dcActTimeCostAmt
,dcActOthCostAmt
,chChargeType
,chStaffCode
,chStaffOfficeCode
,chStaffGroupCode
,chStaffGradeCode
,chStaffEntityCode
,tintStaffIndex
,chFinalBilledFlag
,intGenExpId
,intAssgBudgetRateID
,dtUpdateDateTime
,intFinJnlDtlID
,intTimeSheetHoursID
,dcStdTimeCostAmt
,dcStdOthCostAmt
,dcRevisedBillAmt
,dcRevisedBillHrs
,dtoLastTaggedDatetime
,chLastTaggedStaffCode
,nvcWriteOffComment
,chRegionCode
,chWIPTaggingStatusCode
,intFeeAllocationID"""
    for value in notNullFieldList.split(","):
        print(convert_to_snake_case_new(value))
