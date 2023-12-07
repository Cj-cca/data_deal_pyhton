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


def handle_df(series):
    for value in handleValue:
        v = series[value]
        series[value] = v * 5 if v > 5 else v
    return series


if __name__ == '__main__':
    # 生成随机整数值的 5x5 DataFrame
    handleValue = [1, 2, 3]
    df = pd.DataFrame(np.random.randint(low=0, high=10, size=(5, 5)))
    print(df)
    df = df.apply(handle_df, axis=1)
    print(df)

    # srcFields = ["worker_id", "degree", "first_year_attended", "is_highest_level_of_education", "last_year_attended",
    #              "school_id", "school_name", "education_country", "degree_receiving_date", "field_of_study"
    #              ]
    # tarFields = ["Worker_ID", "School_Name", "Field_Of_Study", "First_Year_Attended", "Last_Year_Attended", "Degree",
    #              "Degree_Receiving_Date", "Is_Highest_Level_of_Education", "Education_Country", "School_ID"
    #              ]
    # databases = 'work_day_stage'
    # tableName = 'ads_hr_workers_education_day_ef'
    # sql_create(srcFields, tarFields, databases, tableName)

    # notNullFieldList = "bintExchangeRateID,ExchangeRateKey,ReportingCurrencyCode,ReportingCurrency,ExchangeRate,ReportingExchangeRate,sdRowCreation"
    # for value in notNullFieldList.split(","):
    #     print(convert_to_snake_case_new(value))
