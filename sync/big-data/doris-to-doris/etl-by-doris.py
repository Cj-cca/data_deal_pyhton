# !/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from enum import Enum
import urllib
from concurrent.futures import ThreadPoolExecutor
import queue
import pyodbc


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
    src_engine = args[0]
    tar_engine = args[1]
    sql = args[2]
    tar_table = args[3]
    srcConn = src_engine.connect()
    pdDataFrame = pd.read_sql(sql, srcConn)
    dataCount = pdDataFrame.to_sql(tar_table, tar_engine, if_exists='append', index=False)
    srcConn.close()
    print("sql execute success:", sql, "。data count: ", dataCount)


def syncDataNormal(src_engine, src_table, tar_engine, tar_table, joint_index, field):
    srcConn = src_engine.connect()
    result = srcConn.execute(text(f"select count(*)as cnt from {src_table}"))
    # 获取第一条元素的第一个字段
    data_count = result.fetchone()[0]
    print(f"{src_table}数据条数: {data_count}")
    gap = 20000
    with MyThreadPoolExecutor(max_workers=5) as t:
        for i in range(0, data_count, gap):
            end = i + gap
            if end > data_count:
                end = data_count
            sql = text(
                f"select {field} from (select ROW_NUMBER() OVER(Order by {joint_index}) AS rowNumber,* from {src_table}) as tbl where tbl.RowNumber >{i} and tbl.RowNumber <={end}")
            t.submit(writeDate, (src_engine, tar_engine, sql, tar_table))
    print("data insert is complete")


def syncDataWithIndex(src_engine, src_table, tar_engine, tar_table, unique_index):
    srcConn = src_engine.connect()
    # 获取元素唯一索引的最大最新值，只限于是递增的索引
    result = srcConn.execute(text(f"select count(1) as cnt from {src_table}"))
    data = result.fetchone()
    data_count = data[0]
    print(f"src data table count:{data_count}")
    gap = 100000
    with MyThreadPoolExecutor(max_workers=4) as t:
        for i in range(0, data_count, gap):
            sql = text(f"""select * from {src_table} order by {unique_index} limit {i},{gap}""")
            t.submit(writeDate, (src_engine, tar_engine, sql, tar_table))
    print("data insert is complete")


def runSyncDataWithIndex():
    # sqlserver
    srcTable = "ods_advisory_talent_link_key_new"
    tarTable = "ods_advisory_talent_link_key_new"
    srcEngine = getEngine("10.158.16.244", "root", "", 9030, EngineType.mysql,
                          "AEL")
    tarEngine = getEngine("10.158.35.241", "admin_user", "6a!F@^ac*jBHtc7uUdxC", 9030, EngineType.mysql,
                          "advisory_engagement_lifecycle")
    # tarEngine = getEngine("10.158.34.175", "root", "", 9030, EngineType.mysql, "PwCMDM")
    uniqueIndex = "booking_id,start_date"
    syncDataWithIndex(srcEngine, srcTable, tarEngine, tarTable, uniqueIndex)


def runSyncDataNormal():
    # sqlserver
    srcTable = "rep.SecurityDetails"
    tarTable = "dwd_security_details_day_ei"
    schema = "rep"
    srcEngine = getEngine("wezcesrpspsmi002.5bb8378829db.database.windows.net", "CHINA_USERS", "PwT1YSNqjiV84NJns24r",
                          1433,
                          EngineType.sqlserver, "CESReporting")
    tarEngine = getEngine("10.158.35.241", "admin_user", "6a!F@^ac*jBHtc7uUdxC", 9030, EngineType.mysql, "ces_new")
    jointIndex = "SecurityId"
    selectField = "SecurityId,SecurityName,IssuerName,PrimarySecurityIdentifierValue,PartyRowId,Symbol,PrimarySecurityIdentifierTypeCvId,SecurityTypeCvId,SecurityClassCvId,DomicileCountryCvId,SecurityStatusCvId,SecurityRestrictionStatusCvId,SecurityFlagTypeCvId,ExchangeCvId,FiscalYearEnd,CreatedDate,MaturityDate,SecurityCorporateActionCvId,SecurityInactiveReasonCvId,AdvisorName,SecurityDataSourceCvId,VerifiedDate,FamilyName,SecurityInactiveDate,CreatedBy,UpdatedBy,UpdateDate,LinkedDate,LinkedBy,IsETLDeleted,RowInsertDateTime,RowUpdateDateTime"
    syncDataNormal(srcEngine, srcTable, tarEngine, tarTable, jointIndex, selectField)


if __name__ == '__main__':
    runSyncDataWithIndex()
