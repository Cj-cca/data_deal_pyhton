# !/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from enum import Enum
import urllib
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote_plus as urlquote
import queue


class MyThreadPoolExecutor(ThreadPoolExecutor):
    """
    重写线程池修改队列数
    """

    def __init__(self, max_workers=None, thread_name_prefix=''):
        super().__init__(max_workers, thread_name_prefix)
        # 队列大小为最大线程数的两倍
        self._work_queue = queue.Queue(self._max_workers * 10)


class EngineType(Enum):
    sqlserver = 1
    mysql = 2


def getEngine(host, user_name, password, port, engine_name, database_name):
    if engine_name == EngineType.sqlserver:
        connUrl = f"mssql+pymssql://{user_name}:%s@{host}:{port}/{database_name}" % (urlquote(password))
    elif engine_name == EngineType.mysql:
        connUrl = f"mysql+pymysql://{user_name}:%s@{host}:{port}/{database_name}" % (urlquote(password))
    else:
        raise RuntimeError("没有该类型的engine,请输入EngineType的类型")
    return create_engine(connUrl)


def truncateTable(engine, table_name):
    tarConn = engine.connect()
    tarConn.execute(text(f"truncate table {table_name}"))
    tarConn.close()
    print("table truncate is complete")


def writeDate(args):
    src_engine = args[0]
    tar_engine = args[1]
    sql = args[2]
    tar_table = args[3]
    srcConn = src_engine.connect()
    try:
        pdDataFrame = pd.read_sql(sql, srcConn)
        print("读取数据成功")
        insert_count = pdDataFrame.to_sql(tar_table, tar_engine, if_exists='append', index=False)
        srcConn.close()
    except Exception as e:
        print("sql执行错误：", e)
    print("sql execute success:", sql, "。data count: ", insert_count)


def syncDataNormal(src_engine, src_table, tar_engine, tar_table, joint_index, field):
    srcConn = src_engine.connect()
    result = srcConn.execute(text(f"select count(*)as cnt from {src_table}"))
    # 获取第一条元素的第一个字段
    data_count = result.fetchone()[0]
    gap = 5000
    with MyThreadPoolExecutor(max_workers=8) as t:
        for i in range(0, data_count, gap):
            end = i + gap
            if end > data_count:
                end = data_count
            sql = text(
                f"select {field} from (select ROW_NUMBER() OVER(Order by {joint_index}) AS rowNumber,* from {src_table}) as tbl where tbl.RowNumber >{i} and tbl.RowNumber <={end}")
            t.submit(writeDate, (src_engine, tar_engine, sql, tar_table))
    print("data insert is complete")


def syncDataWithIndex(src_engine, src_table, tar_engine, tar_table, unique_index, field):
    srcConn = src_engine.connect()
    # 获取元素唯一索引的最大最新值，只限于是递增的索引
    result = srcConn.execute(text(f"select max({unique_index}) as max,min({unique_index}) as min from {src_table}"))
    data = result.fetchone()
    max = data[0]
    min = data[1]
    gap = 10000
    with MyThreadPoolExecutor(max_workers=8) as t:
        for i in range(min - 1, max, gap):
            tmp_end = i + gap
            if tmp_end > max:
                tmp_end = max
            sql = text(f"select {field} from {src_table} where {unique_index} > {i} and  {unique_index} <= {tmp_end}")
            t.submit(writeDate, (src_engine, tar_engine, sql, tar_table))
    print("data insert is complete")


def runSyncDataWithIndex():
    # sqlserver
    srcTable = "PartyDetail"
    tarTable = "PartyDetail"
    # srcEngine = getEngine("CNCSQLPWV5028", "App_Airflow", "P@ss1234567890", 1800, EngineType.sqlserver, "PwCMDM")
    srcEngine = getEngine("10.158.34.175", "root", "", 9030, EngineType.mysql, "CES")
    tarEngine = getEngine("10.158.16.244", "root", "", 9030, EngineType.mysql, "CES")
    uniqueIndex = "PartyRowId"
    selectField = "PartyRowId,Prid,PartyId,DUNS,PartyName,PartyType,PartyStatus,PartyLifeCycleStatus,OrgType,Channel," \
                  "EntityInactiveStatusReason,PwCIndustry,AddressLine1,AddressLine2,City,RegisteredAddressState," \
                  "RegisteredAddressPostalCode,RegisteredCountry,RegisteredCountryDesc,OperatingAddressLine1," \
                  "OperatingAddressLine2,OperatingCity,OperatingAddressState,OperatingAddressPostalCode," \
                  "OperatingCountry,OperatingCountryDesc,PartyTypeDesc,PartyStatusDesc,PartyLifeCycleStatusDesc," \
                  "OrgTypeDesc,ChannelDesc,PwCIndustryDesc,GUPIndicator,IsGUPofAnyType,CreationDate," \
                  "EntityInactivatedDate,EntitySunsettedDate,IndependenceEntityType,PwCSegment,PwCSegmentDesc," \
                  "IndependenceStandard,IndependenceStandardCvId,EntityRestrictionStatusDesc," \
                  "EntityInactiveStatusReasonDesc,IsAICPA,IsGIP,IsSEC,ISSoS,IsFIN,ClientInternalIds," \
                  "ClientInternalType,ChinaCICPA,IsAuditClient,IRPPersonId,IsIRPOverridden,GRPPersonId,GRPPersonName," \
                  "HasSecurity,HasDeliverable,CSI300,Fortune500,FortuneGlobal500,FTEmerging500,FTEuro500,FTGlobal500," \
                  "FTSE350,HangSeng50,NIKKEI225,SPAsia500,SPLatinAmerica40,EntityVerificationTerritory," \
                  "EntityVerificationStatus,EntityVerificationDate,EntityPIEDesignation,EntityEUPublicInterestEntity," \
                  "EUNonAuditServicesRestrictionsApply,RestrictionStartDate,CreatedBy,UpdateDate,UpdatedBy,IsEUPIE," \
                  "IsCountry,IsIndiaCA,IsETLDeleted,LineageId,RowInsertDateTime,RowUpdateDateTime,OperationalStatus," \
                  "EntityRestrictionStatusCvId,ETL_time "
    truncateTable(tarEngine, tarTable)
    syncDataWithIndex(srcEngine, srcTable, tarEngine, tarTable, uniqueIndex, selectField)


def runSyncDataNormal():
    # sqlserver
    srcTable = "ods_advisory_talent_link"
    tarTable = "ODS_ADVISORY_TALENT_LINK"
    # srcEngine = getEngine("CNCSQLPWV5028", "App_Airflow", "P@ss1234567890", 1800, EngineType.sqlserver, "PwCMDM")
    # tarEngine = getEngine("10.158.16.244", "root", "", 9030, EngineType.mysql, "PwCMDM")
    srcEngine = getEngine("10.158.35.241", "admin_user", "6a!F@^ac*jBHtc7uUdxC", 9030, EngineType.mysql,
                          "advisory_engagement_lifecycle")
    tarEngine = getEngine("10.157.112.167", "oats_talentlink", "Fo@tI%Vwc(AO", 3306, EngineType.mysql, "AEL")
    jointIndex = "staff_id,job_id,employee_id,start_date"
    selectField = "staff_id,job_id,employee_id,start_date,country_code,worker_id,office_code,job_code,client_code,holiday_flag,work_hours,loading,end_date,term_flag,staff_name,job_title,create_by_date"
    syncDataNormal(srcEngine, srcTable, tarEngine, tarTable, jointIndex, selectField)


if __name__ == '__main__':
    runSyncDataNormal()
