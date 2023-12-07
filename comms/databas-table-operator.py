# !/usr/bin/env python
# -*- coding: utf-8 -*-
import pymysql
import re
import pandas as pd
from sqlalchemy import text
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote


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


def table_exist(cursor, tableName):
    # tarCursor.execute(f"use {tableName.split('.')[0]}")
    cursor.execute("show tables")
    dataList = cursor.fetchall()
    tableList = re.findall('(\'.*?\')', str(dataList))
    tableExistList = [re.sub("'", '', each) for each in tableList if each.replace("'", '') == tableName]
    return len(tableExistList)


# 在目标数据源创建原始数据源的指定数据库
def createTableIfTableNotExist(orgCursor, tarCursor, tableName):
    exist = table_exist(tarCursor, tableName)
    if exist == 0:
        execSql = f"show create table {tableName}"
        orgCursor.execute(execSql)
        res = orgCursor.fetchall()
        sql = res[0]["Create Table"]
        # dataBaseName = tableName.split(".")[0]
        # tarCursor.execute(f"use {dataBaseName}")
        tarCursor.execute(sql)
        print("数据源：", tableName, "，创建成功")
    else:
        print("数据源：", tableName, "，已经存在")


def create_table(tar_cursor, table_key):
    ods_table_name = 'ods_' + table_key + '_day_ei'
    ods_detail_table_name = 'ods_' + table_key + '_detail_day_ei'
    ods_sql = f"""
            CREATE TABLE {ods_table_name} (
          `page_index` int(11) NULL COMMENT "",
          `create_time` datetime NULL COMMENT "",
          `response_data` text NULL COMMENT "",
          `response_object_count` int(11) NULL COMMENT "",
          `query_url` text NULL COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY(`page_index`, `create_time`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`page_index`, `create_time`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    tar_cursor.execute(ods_sql)
    print("数据源：", ods_table_name, "，创建成功")

    ods_detail_sql = f"""
            CREATE TABLE {ods_detail_table_name} (
          `batch_id` int(11) NULL COMMENT "",
          `create_time` datetime NULL COMMENT "",
          `current_page_num` int(11) NULL COMMENT "",
          `response_data` text NULL COMMENT "",
          `response_object_num` int(11) NULL COMMENT "",
          `query_url` text NULL COMMENT "",
          `comment` varchar(1000) NULL COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY(`batch_id`, `create_time`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`batch_id`, `create_time`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    tar_cursor.execute(ods_detail_sql)
    print("数据源：", ods_detail_table_name, "，创建成功")


def execute_sql(tar_engine):
    sql = text(f"select * from dwd_termination_and_transfer_day_ef")
    srcConn = tar_engine.connect()
    dataFrame = pd.read_sql(sql, srcConn)
    dataFrame.loc[0][
        'worker_id'] = '1010088888888888597777777777777777777777777777779999999999999999999999999999999999999999999999999999999900000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000099999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999998333'
    count = -1
    try:
        count = dataFrame.to_sql("dwd_termination_and_transfer_day_ef", tar_engine, if_exists='append', index=False)
    except Exception as e:
        print('5025' in str(e.__dict__['orig']).split(',')[0])
    print("数据同步完成，条数：", count)


def truncateTable(engine, table_name):
    tarConn = engine.connect()
    tarConn.execute(text(f"truncate table {table_name}"))
    tarConn.close()
    print("table truncate is complete")


if __name__ == '__main__':
    tableNameList = "ads_hr_workers_certification_day_ef,ads_hr_workers_education_day_ef,dwd_existing_staff_day_ef,dwd_existing_staff_day_st,dwd_hr_job_profiles_day_ef,dwd_hr_job_profiles_day_st,dwd_hr_organization_day_ef,dwd_hr_organization_day_st,dwd_hr_staff_transfer_from_location_day_ef,dwd_hr_staff_transfer_from_location_day_st,dwd_hr_staff_transfer_to_location_day_ef,dwd_hr_staff_transfer_to_location_day_st,dwd_hr_workers_certification_day_ef,dwd_hr_workers_certification_day_st,dwd_hr_workers_education_day_ef,dwd_hr_workers_education_day_st,dwd_new_hire_or_secondment_day_ef,dwd_new_hire_or_secondment_day_st,dwd_termination_and_transfer_day_ef,dwd_termination_and_transfer_day_st,ods_existing_staff_day_ei,ods_existing_staff_detail_day_ei,ods_hr_api_data_exception_records,ods_hr_job_profiles_day_ei,ods_hr_job_profiles_detail_day_ei,ods_hr_organization_day_ei,ods_hr_organization_detail_day_ei,ods_hr_staff_transfer_from_location_day_ei,ods_hr_staff_transfer_from_location_detail_day_ei,ods_hr_staff_transfer_to_location_day_ei,ods_hr_staff_transfer_to_location_detail_day_ei,ods_hr_table_mapping,ods_hr_task_complete_record,ods_hr_workers_certification_day_ei,ods_hr_workers_certification_detail_day_ei,ods_hr_workers_education_day_ei,ods_hr_workers_education_detail_day_ei,ods_new_hire_or_secondment_day_ei,ods_new_hire_or_secondment_detail_day_ei,ods_termination_and_transfer_day_ei,ods_termination_and_transfer_detail_day_ei"
    # orgCursor = DorisConnection.getCursor(host="10.158.16.244", user="root", password="",
    #                                       database="procurement_all")
    # orgCursor = DorisConnection.getCursor(host="10.158.35.241", user="admin_user", password="6a!F@^ac*jBHtc7uUdxC",
    #                                       database="pwc_mdm")
    # tarCursor = DorisConnection.getCursor(host="10.158.35.241", user="admin_user", password="6a!F@^ac*jBHtc7uUdxC",
    #                                       database="finance_bi")

    tarEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/work_day_stage"
    )
    for tableName in tableNameList.split(","):
        truncateTable(tarEngine, tableName)
    # tarEngine = create_engine(
    #     f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/work_day_stage"
    # )
    # execute_sql(tarEngine)
