# !/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import json
import re
import zlib
import base64
import pandas
from sqlalchemy import create_engine
from sqlalchemy import text

resultCertificationColumn = ['Worker_ID',
                             'Issued_Date',
                             'Certification_Reference_ID',
                             'Certification_Name',
                             'Expiration_Date',
                             'Examination_Score',
                             'Examination_Date', ]

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
                'd.results.cust_anothercertificates.results.cust_certificationNav.externalName_en_US': 'Certification_Name',
                'd.results.cust_anothercertificates.results.cust_expirationdate': 'Expiration_Date',
                'd.results.cust_anothercertificates.results.cust_scores': 'Examination_Score',
                'd.results.cust_anothercertificates.results.cust_examinationdate': 'Examination_Date'}

tarCertificationTableNameOds = "ExistingStaff_ods"
tarCertificationTableNameOdsDetail = "HRWorkers_Certification_ods_detail"
tarCertificationTableNameDwd = "HRWorkers_Certification_dwc"

tarEngine = create_engine('mysql+pymysql://root@10.158.16.244:9030/WorkDayStage')
batchID = 0
createTime = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


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
    print(f"{tarCertificationTableNameDwd}数据插入成功，受影响行数：", insert_count)


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
    datetime = '2023-06-13 00:00:00'
    conn = tarEngine.connect()
    data_frame = pandas.read_sql(text(f"select * from {tarCertificationTableNameOds} where create_time=\'{datetime}\'"), conn)
    for i in range(0, len(data_frame.index)):
        query_url = data_frame.loc[i, 'query_url']
        page_index = data_frame.loc[i, 'page_index']
        compressed_str = data_frame.loc[i, 'response_data']
        compressed_data = base64.b64decode(compressed_str)
        data = zlib.decompress(compressed_data).decode('utf-8')
        # deal_ods_detail(data, query_url, page_index)
        # deal_certification_dwd(data)
