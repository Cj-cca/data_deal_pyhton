# !/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from enum import Enum
import urllib
from concurrent.futures import ThreadPoolExecutor
import queue

tarFieldMap = {'StaffID': 'staff_id', 'NotesName': 'notes_name', 'StartDate': 'start_date', 'EndDate': 'end_date',
               'Message': 'message', 'OONStatus': 'oon_status', 'RequestStatus': 'request_status',
               'LastModifiedDate': 'last_modified_date', 'RepeatInterval': 'repeat_interval'}


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


def truncateTable(engine, table_name):
    tarConn = engine.connect()
    tarConn.execute(text(f"truncate table {table_name}"))
    tarConn.close()
    print("table truncate is complete")


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
    try:
        pdDataFrame = pd.read_sql(sql, srcConn)
        pdDataFrame.rename(columns=tarFieldMap, inplace=True)
        print("读取数据成功")
        data_count = pdDataFrame.to_sql(tar_table, tar_engine, if_exists='append', index=False)
        print("sql execute success:", sql, "。data count: ", data_count)
        srcConn.close()
    except Exception as e:
        print("sql执行错误：", e)


def fullSyncDataNormal(src_engine, src_table, tar_engine, tar_table, joint_index, field):
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


def fullSyncDataWithIndex(src_engine, src_table, tar_engine, tar_table, unique_index, field):
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


def increaseSyncDataWithNormal(src_engine, src_table, tar_engine, tar_table, order_field,
                               field, src_filter_field, tar_filter_field):
    tarConn = tar_engine.connect()
    conditionResult = tarConn.execute(
        text(f"select {tar_filter_field} from {tar_table} order by {tar_filter_field} desc limit 1"))
    condition = str(conditionResult.fetchone()[0])
    srcConn = src_engine.connect()
    result = srcConn.execute(text(f"select count(*)as cnt from {src_table} where {src_filter_field}>=\'{condition}\'"))
    # 获取第一条元素的第一个字段
    data_count = result.fetchone()[0]
    gap = 5000
    with MyThreadPoolExecutor(max_workers=8) as t:
        for i in range(0, data_count, gap):
            end = i + gap
            sql = text(
                f"select {field} from "
                f"("
                f"select ROW_NUMBER() OVER(Order by {order_field}) AS rowNumber,* from {src_table} "
                f"where {src_filter_field}>=\'{condition}\'"
                f") as tbl "
                f"where tbl.RowNumber >{i} and tbl.RowNumber <={end}")
            t.submit(writeDate, (src_engine, tar_engine, sql, tar_table))
    print("data insert is complete")


def runSyncDataNormal():
    srcTable = "dbo.OutOfOfficeNotification"
    tarTable = "ods_gts_out_of_office_notification_hour_ei"
    srcEngine = getEngine("hksqlpwv314", "App_vContactsIM", "P@ss1234", 1800, EngineType.sqlserver, "PwCContacts")
    tarEngine = getEngine("10.158.35.241", "admin_user", "6a!F@^ac*jBHtc7uUdxC", 9030, EngineType.mysql, "pwc_contacts")
    orderField = "StaffID"
    srcFilterField = "LastModifiedDate"
    tarFilterField = "last_modified_date"
    selectField = "replace(StaffID,' ','')as StaffID,NotesName,StartDate,EndDate,Message,OONStatus,RequestStatus," \
                  "LastModifiedDate,RepeatInterval "
    # 只有第一次同步的时候执行
    # truncateTable(tarEngine, tarTable)
    # fullSyncDataNormal(srcEngine, srcTable, tarEngine, tarTable, jointIndex, selectField)
    increaseSyncDataWithNormal(srcEngine, srcTable, tarEngine, tarTable, orderField, selectField, srcFilterField,
                               tarFilterField)


if __name__ == '__main__':
    runSyncDataNormal()
