import time

import pandas as pd
import queue
import os
import datetime
from multiprocessing import SimpleQueue
from sqlalchemy import create_engine
from sqlalchemy import text
import urllib
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
import pymssql
import re

# if __name__ == '__main__':
#     map = {}
#     list = {1,2,3,4}
#     arr = [1,2,3,4,5]
#     map["source"] = {}
#     map["target"] = {}
#     map["source"]["age"] = "austin"
#     map["source"]["year"] = 23
#     map["source"]["height"] = 168
#     map["source"]["weight"] = 130
#     map["target"]["age"] = "jan"
#     map["target"]["year"] = 25
#     map["target"]["height"] = 168
#     map["target"]["weight"] = 130
#     personalProfile = f'my name is {map["source"]["age"]}, i am {map["source"]["year"]} years old, i am height is { map["source"]["height"]} and weight is { map["source"]["weight"]}'
#     print(personalProfile)
from sqlalchemy.dialects import mysql


class DorisConnection:
    conn = None
    cursor = None

    @staticmethod
    def getCursor(**kwargs):
        # 链接服务端
        if "port" not in kwargs or kwargs['port'] is None:
            kwargs['port'] = 9030
        if "charset" not in kwargs or kwargs['charset'] is None:
            kwargs['charset'] = "utf8"
        DorisConnection.conn = pymysql.connect(
            host=kwargs['host'],  # MySQL服务端的IP地址
            port=kwargs['port'],  # MySQL默认PORT地址(端口号)
            user=kwargs['user'],  # 用户名
            password=kwargs['password'],  # 密码,也可以简写为passwd
            database=kwargs['database'],  # 库名称,也可以简写为db
            charset=kwargs['charset']  # 字符编码
        )
        # 产生获取命令的游标对象
        # cursor = conn_obj.cursor()  # 括号内不写参数,数据是元组套元组
        DorisConnection.cursor = DorisConnection.conn.cursor(cursor=pymysql.cursors.DictCursor)  # 括号内写参数,数据会处理成字典形式
        return DorisConnection.cursor

    @staticmethod
    def close():
        if DorisConnection.cursor is not None:
            DorisConnection.cursor.close()
            DorisConnection.cursor = None

        if DorisConnection.conn is not None:
            DorisConnection.conn.close()
            DorisConnection.conn = None


def calculate_circle(upEdge, downEdge, height=5):
    area = 1 / 2 * (upEdge + downEdge) * height
    return area


def table_exist(cursor, tableName):
    # tarCursor.execute(f"use {tableName.split('.')[0]}")
    cursor.execute("show tables")
    dataList = cursor.fetchall()
    tableList = re.findall('(\'.*?\')', str(dataList))
    tableExistList = [re.sub("'", '', each) for each in tableList if each.replace("'", '') == tableName.split(".")[1]]
    return len(tableExistList)


# 在目标数据源创建原始数据源的指定数据库
def creatTableIfTableNotExist(orgCursor, tarCursor, tableName):
    exist = table_exist(tarCursor, tableName)
    if exist == 0:
        execSql = f"show create table {tableName}"
        orgCursor.execute(execSql)
        res = orgCursor.fetchall()
        sql = res[0]["Create Table"]
        dataBaseName = tableName.split(".")[0]
        tarCursor.execute(f"use {dataBaseName}")
        tarCursor.execute(sql)
        print("数据源：", tableName, "，创建成功")
    else:
        print("数据源：", tableName, "，已经存在")


def synSqlServerTable(srcEngine, srcTable, tarEngine, tarTable):
    srcConn = srcEngine.connect()
    tarConn = tarEngine.connect()
    pdSrc = pd.read_sql(text(f"select top 1 * from {srcTable}"), srcConn)
    tarConn.execute(text(f"truncate table {tarTable}"))
    print("table truncate operator complete")
    respondCount = pdSrc.to_sql(tarTable, tarEngine, if_exists='append', index=False)
    print("data update operator complete，完成条数：", respondCount)


def updateHistoryTable():
    print()


def main():
    # CNCSQLPWV5027, 1800
    # Account： MiddlePlatform
    # Password: !QAZ@WSX#edc
    # srcConnUrl = "mssql+pymssql://MiddlePlatform:%s@CNCSQLPWV5027:1800/PwCMDM?charset=utf8" % (urllib.parse.quote_plus('!QAZ@WSX#edc'))
    srcConnUrl = "mssql+pymssql://App_Airflow:%s@CNCSQLPWV5028:1800/PwCMDM" % (
        urllib.parse.quote_plus('P@ss1234567890'))
    tarConnUrl = "mysql+pymysql://root@10.158.16.244:9030/PwCMDM"
    srcTable = "dbo.tblPracticeEntity"
    tarTable = "dbo_tblPracticeEntity"
    srcEngine = create_engine(srcConnUrl)
    tarEngine = create_engine(tarConnUrl)
    synSqlServerTable(srcEngine, srcTable, tarEngine, tarTable)


def deleteData():
    conn = create_engine('mysql+pymysql://root@10.158.16.244:9030/PwCMDM').connect()
    res = conn.execute(text("truncate table dbo_vwFirmUserRole"))
    print(res)


def etlPwCMDMData(engine):
    sqlList = ["""insert into `PwCMDM`.`dbo_vwFirmUserRole_his`(startDate, ID, StaffID, StaffName, RoleCode, RoleDesc, OfficeCode, GroupCode, GroupDesc, LoSCode, LoSDesc, CountryCode, OUStatus, daTerminationDate, endDate)
SELECT his.startDate,his.ID,his.StaffID,his.StaffName,his.RoleCode,his.RoleDesc,his.OfficeCode,his.GroupCode,his.GroupDesc,his.LoSCode,his.LoSDesc,his.CountryCode,his.OUStatus,his.daTerminationDate,CURDATE() as endDate
FROM (select * from PwCMDM.dbo_vwFirmUserRole_his where endDate = '9999-12-31') AS his LEFT JOIN PwCMDM.dbo_vwFirmUserRole AS org
ON his.ID = org.ID where org.ID IS NULL;""", """insert into `PwCMDM`.`dbo_vwFirmUserRole_his`(startDate, ID, StaffID, StaffName, RoleCode, RoleDesc, OfficeCode, GroupCode, GroupDesc, LoSCode, LoSDesc, CountryCode, OUStatus, daTerminationDate, endDate)
select his.startDate, his.ID, his.StaffID, his.StaffName, his.RoleCode, his.RoleDesc, his.OfficeCode, his.GroupCode, his.GroupDesc, his.LoSCode, his.LoSDesc, his.CountryCode, his.OUStatus, his.daTerminationDate,CURDATE() as endDate
from(
     SELECT  ID,StaffID,StaffName,RoleCode,RoleDesc,OfficeCode,GroupCode,GroupDesc,LoSCode,LoSDesc,CountryCode,OUStatus,daTerminationDate
	FROM `PwCMDM`.`dbo_vwFirmUserRole` EXCEPT SELECT ID,StaffID,StaffName,RoleCode,RoleDesc,OfficeCode,GroupCode,GroupDesc,LoSCode,LoSDesc,CountryCode,OUStatus,daTerminationDate
    FROM `PwCMDM`.`dbo_vwFirmUserRole_his` WHERE endDate = '9999-12-31'
        ) as orgUpdate
join (select * from PwCMDM.dbo_vwFirmUserRole_his where endDate = '9999-12-31') AS his
    on orgUpdate.ID = his.ID;""", """insert into `PwCMDM`.`dbo_vwFirmUserRole_his`(startDate, ID, StaffID, StaffName, RoleCode, RoleDesc, OfficeCode, GroupCode, GroupDesc, LoSCode, LoSDesc, CountryCode, OUStatus, daTerminationDate, endDate)
select date_add(curdate(), interval 1 day ) as startDate,orgNew.*,'9999-12-31' as endDate
from(
     SELECT  ID,StaffID,StaffName,RoleCode,RoleDesc,OfficeCode,GroupCode,GroupDesc,LoSCode,LoSDesc,CountryCode,OUStatus,daTerminationDate
	FROM `PwCMDM`.`dbo_vwFirmUserRole` EXCEPT SELECT ID,StaffID,StaffName,RoleCode,RoleDesc,OfficeCode,GroupCode,GroupDesc,LoSCode,LoSDesc,CountryCode,OUStatus,daTerminationDate
    FROM `PwCMDM`.`dbo_vwFirmUserRole_his` WHERE endDate = '9999-12-31'
        ) as orgNew
left join (select * from PwCMDM.dbo_vwFirmUserRole_his where endDate = '9999-12-31') AS his
    on orgNew.ID = his.ID
where his.ID is null;"""]
    conn = engine.connect()
    for sql in sqlList:
        res = conn.execute(text(sql.replace("\n", " ")))
        print(sql, ",执行成功,受影响的行数: ", res.rowcount)


def readSqlServerForPymssql():
    db = pymssql.connect(host='CNCSQLPWV5027',
                         user='MiddlePlatform',
                         password='!QAZ@WSX#edc',
                         database='PwCMDM',
                         port=1800)
    cursor = db.cursor()
    cursor.execute("select top 1 * from Common.FirmUserRole")
    res = cursor.fetchall
    print(res)


def readSqlServerForEngine():
    srcConnUrl = "mssql+pymssql://App_Airflow:%s@CNCSQLPWV5028:1800/PwCMDM" % (
        urllib.parse.quote_plus('P@ss1234567890'))
    engine = create_engine(srcConnUrl)
    srcConn = engine.connect()
    pdSrc = pd.read_sql(text("select top 1 * from  dbo.tblPracticeEntity"), srcConn)
    print(pdSrc)


class MyThreadPoolExecutor(ThreadPoolExecutor):
    """
    重写线程池修改队列数
    """

    def __init__(self, max_workers=None, thread_name_prefix=''):
        super().__init__(max_workers, thread_name_prefix)
        # 队列大小为最大线程数的两倍
        self._work_queue = queue.Queue(max_workers * 2)


def format_date(date):
    result = ''
    if date == '':
        return result
    if len(date) == 4:
        date_str = f"{date}-01-01"
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").strftime("%m/%d/%Y %I:%M:%S %p")
    return result


def convert_to_snake_case(string):
    # 使用正则表达式将字符串中的单词分割
    words = re.findall('[A-Za-z][a-z]*', string)
    # 将单词转换为小写并使用下划线连接
    snake_case = '_'.join(word.lower() for word in words)
    return snake_case


def convert_to_snake_case_new(string):
    tmp = re.sub(r'([A-Z]{2,})', lambda x: x.group().lower().title(), string)
    words = re.findall('[A-Za-z][a-z]*', tmp)
    result = '_'.join(word.lower() for word in words)
    return result


def match_str(tar_str, fields):
    for field in fields:
        str_new = tar_str.replace("_", "")
        field_new = field.replace("_", "")
        match = re.search(field_new, str_new, re.IGNORECASE)
        if match:
            return field
    print("没有找到该字符串：", tar_str)
    return ''


def change_map(map_data):
    if 'a' in map_data:
        map_data['a'] = 3
    map_data['cc'] = 00


def spider(page):
    time.sleep(3)
    print(f"crawl task{page} finished")


f = ['A', 'C']


def handle_series(x):
    for i in f:
        if x[i] > 5:
            x[i] = x[i] + 1
    return x


def a():
    x = (6241.5 + 6241.5*1.5)/2*15*0.01


if __name__ == '__main__':
    dit = {"A": {"a": "str"}}

    # execSql = f"show create table aaa"
    # orgCursor = DorisConnection.getCursor(host="CNCSQLPWV5027", user="M·iddlePlatform", password="!QAZ@WSX#edc", port=1800, database=dataBase)
    # tarCursor = DorisConnection.getCursor(host="10.158.16.244", user="root", password="", database="security")
    # for tableName in tableNameList.split(","):
    #     creatTableIfTableNotExist(orgCursor, tarCursor, tableName)
    # testSqlServer()
    # engine = create_engine('mysql+pymysql://root@10.158.16.244:9030/PwCMDM')
    # result = engine.connect().execute(text("select * from Opportunity_tblProductSegment limit 1"))
    # print()
    # etlPwCMDMData(engine)
    # with ThreadPoolExecutor(max_workers=5) as t:
    #     for i in range(0, 10000000):
    #         t.submit(circle)
    #
    #     print("done")
    # readSqlServerForEngine()
    # with MyThreadPoolExecutor(max_workers=2) as executor:
    #     for i in range(0, 20):
    #         executor.submit(task, i)
