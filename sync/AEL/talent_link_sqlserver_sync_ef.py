import pandas as pd
from sqlalchemy import text
from sqlalchemy import create_engine
from datetime import datetime, timedelta

import pymysql
import urllib


def run_new(sql, src_engine_conn, inset_fields):
    print("开始创建连接")
    connection = pymysql.connect(host="10.157.112.167",
                                 port=3306,
                                 user="oats_talentlink",
                                 password="Fo@tI%Vwc(AO",
                                 db="AEL",
                                 charset="utf8")
    cursor = connection.cursor()
    print("连接创建完成")
    df = pd.read_sql(sql, src_engine_conn)
    print("数据读取成功，开始同步数据")
    try:
        for index, row in df.iterrows():
            query = f"""
            INSERT INTO AEL.ODS_ADVISORY_TALENT_LINK ({inset_fields}) 
            VALUES (%s,%s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s) 
            ON DUPLICATE KEY UPDATE staff_id = VALUES(staff_id),job_id = VALUES(job_id),employee_id = VALUES(employee_id),
            country_code = VALUES(country_code),worker_id = VALUES(worker_id),
            office_code = VALUES(office_code),job_code = VALUES(job_code),client_code = VALUES(client_code),
            holiday_flag = VALUES(holiday_flag),work_hours = VALUES(work_hours),
            loading = VALUES(loading),end_date = VALUES(end_date),term_flag = VALUES(term_flag),
            staff_name = VALUES(staff_name),job_title = VALUES(job_title)
"""
            values = (row['booking_id'], row['start_date'], row['staff_id'], row['job_id'], row['employee_id'],
                      row['country_code'], row['worker_id'], row['office_code'], row['job_code'], row['client_code'],
                      row['holiday_flag'], row['work_hours'], row['loading'], row['end_date'], row['term_flag'],
                      row['staff_name'], row['job_title'])
            cursor.execute(query, values)
    except Exception as e:
        print(e)
    finally:
        connection.commit()
        print("数据插入成功")
        cursor.close()
        connection.close()


if __name__ == '__main__':
    table_name = "ods_advisory_talent_link"
    select_fields = "booking_id,start_date, staff_id,job_id,employee_id,country_code,worker_id,office_code,job_code,client_code,holiday_flag,work_hours,loading,end_date,term_flag,staff_name,job_title"
    joint_index = "booking_id,start_date"
    srcEngine = create_engine(
        f'mysql+pymysql://admin_user:{urllib.parse.quote_plus("6a!F@^ac*jBHtc7uUdxC")}@10.158.35.241:9030/advisory_engagement_lifecycle')
    print("任务开始执行")
    srcConn = srcEngine.connect()
    result = srcConn.execute(text(
        f"select count(*)as cnt from {table_name}"))
    # 获取第一条元素的第一个字段
    data_count = result.fetchone()[0]
    print("doris数据量查询完成")
    gap = 3000
    for i in range(0, data_count, gap):
        end = i + gap
        if end > data_count:
            end = data_count
        sql_main = text(
            f"select {select_fields} from (select ROW_NUMBER() OVER(Order by {joint_index}) AS rowNumber,* from {table_name}) as tbl where tbl.RowNumber >{i} and tbl.RowNumber <={end}")
        print("开始执行当前批次数据")
        run_new(sql_main, srcConn, select_fields)

    srcConn.close()
    print("数据同步完成")
