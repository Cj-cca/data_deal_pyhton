# !/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine
from enum import Enum
import urllib
from concurrent.futures import ThreadPoolExecutor
import pymssql
import concurrent.futures

startDate = '2023-6-1'
endDate = '2023-7-1'

fieldMapping = {
    "SecurityId": "security_id",
    "SecurityName": "security_name",
    "IssuerName": "issuer_name",
    "PrimarySecurityIdentifierValue": "primary_security_identifier_value",
    "PartyRowId": "party_row_id",
    "Symbol": "symbol",
    "PrimarySecurityIdentifierTypeCvId": "primary_security_identifier_type_cv_id",
    "SecurityTypeCvId": "security_type_cv_id",
    "SecurityClassCvId": "security_class_cv_id",
    "DomicileCountryCvId": "domicile_country_cv_id",
    "SecurityStatusCvId": "security_status_cv_id",
    "SecurityRestrictionStatusCvId": "security_restriction_status_cv_id",
    "SecurityFlagTypeCvId": "security_flag_type_cv_id",
    "ExchangeCvId": "exchange_cv_id",
    "FiscalYearEnd": "fiscal_year_end",
    "CreatedDate": "created_date",
    "MaturityDate": "maturity_date",
    "SecurityCorporateActionCvId": "security_corporate_action_cv_id",
    "SecurityInactiveReasonCvId": "security_inactive_reason_cv_id",
    "AdvisorName": "advisor_name",
    "SecurityDataSourceCvId": "security_data_source_cv_id",
    "VerifiedDate": "verified_date",
    "FamilyName": "family_name",
    "SecurityInactiveDate": "security_inactive_date",
    "CreatedBy": "created_by",
    "UpdatedBy": "updated_by",
    "UpdateDate": "update_date",
    "LinkedDate": "linked_date",
    "LinkedBy": "linked_by",
    "IsETLDeleted": "is_etl_deleted",
    "RowInsertDateTime": "row_insert_date_time",
    "RowUpdateDateTime": "row_update_date_time"
}


class EngineType(Enum):
    sqlserver = 1
    mysql = 2


def getEngine(host, userName, passWord, port, engineName, databaseName):
    if engineName == EngineType.sqlserver:
        connUrl = f"mssql+pymssql://{userName}:%s@{host}:{port}/{databaseName}" % (urllib.parse.quote_plus(passWord))
    elif engineName == EngineType.mysql:
        connUrl = f"mysql+pymysql://{userName}:%s@{host}:{port}/{databaseName}" % (
            urllib.parse.quote_plus(passWord))
    else:
        raise RuntimeError("没有该类型的engine,请输入EngineType的类型")
    return create_engine(connUrl)


def writeDate(args):
    src_conn = pymssql.connect(server='wezcesrpspsmi002.5bb8378829db.database.windows.net',
                               port='1433',
                               user='CHINA_USERS',
                               password='PwT1YSNqjiV84NJns24r',
                               database='CESReporting')
    tar_engine = args[0]
    sql = args[1]
    tar_table = args[2]
    pdDataFrame = pd.read_sql(sql, src_conn)
    pdDataFrame.rename(columns=fieldMapping, inplace=True)
    dataCount = pdDataFrame.to_sql(tar_table, tar_engine, if_exists='append', index=False)
    src_conn.close()
    print("sql execute success:", sql, "。data count: ", dataCount)


def syncDataNormal(src_conn, src_table, tar_engine, tar_table, joint_index, field):
    cur = src_conn.cursor()
    cur.execute(f"select count(*)as cnt from {src_table} where UpdateDate >=\'{startDate}\' and UpdateDate<\'{endDate}\'")
    # 获取第一条元素的第一个字段
    data_count = cur.fetchone()[0]
    print(f"{src_table}数据条数: {data_count}")
    src_conn.close()
    gap = 5000
    count = 0
    tasks = []
    with ThreadPoolExecutor(max_workers=5) as t:
        for i in range(0, data_count, gap):
            end = i + gap
            if end > data_count:
                end = data_count
            sql = (f"select {field} from ("
                   f"select ROW_NUMBER() OVER(Order by {joint_index}) AS rowNumber,* from {src_table} where UpdateDate >=\'{startDate}\' and UpdateDate <\'{endDate}\'"
                   f") as tbl "
                   f"where tbl.RowNumber >{i} and tbl.RowNumber <={end}")
            task = t.submit(writeDate, (tar_engine, sql, tar_table))
            tasks.append(task)
            count += 1
            if count % 20 == 0:
                print("等待20个任务执行完成")
                concurrent.futures.wait(tasks)
                print("20个任务执行完成")
                tasks = []
    print("所有任务执行完成")


def runSyncDataNormal():
    # sqlserver
    srcTable = "rep.SecurityDetails"
    tarTable = "dwd_security_details_day_ei"
    src_conn = pymssql.connect(server='wezcesrpspsmi002.5bb8378829db.database.windows.net',
                               port='1433',
                               user='CHINA_USERS',
                               password='PwT1YSNqjiV84NJns24r',
                               database='CESReporting')
    tarEngine = getEngine("10.158.35.241", "admin_user", "6a!F@^ac*jBHtc7uUdxC", 9030, EngineType.mysql, "ces_new")
    jointIndex = "SecurityId"
    selectField = "SecurityId,SecurityName,IssuerName,PrimarySecurityIdentifierValue,PartyRowId,Symbol,PrimarySecurityIdentifierTypeCvId,SecurityTypeCvId,SecurityClassCvId,DomicileCountryCvId,SecurityStatusCvId,SecurityRestrictionStatusCvId,SecurityFlagTypeCvId,ExchangeCvId,FiscalYearEnd,CreatedDate,MaturityDate,SecurityCorporateActionCvId,SecurityInactiveReasonCvId,AdvisorName,SecurityDataSourceCvId,VerifiedDate,FamilyName,SecurityInactiveDate,CreatedBy,UpdatedBy,UpdateDate,LinkedDate,LinkedBy,IsETLDeleted,RowInsertDateTime,RowUpdateDateTime"
    syncDataNormal(src_conn, srcTable, tarEngine, tarTable, jointIndex, selectField)


if __name__ == '__main__':
    print("开始执行任务")
    runSyncDataNormal()
    print("任务执行完成")
