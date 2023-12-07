import pandas as pd
from sqlalchemy import text
from sqlalchemy import create_engine
from datetime import datetime, timedelta

import pymysql
import urllib

fields = ['staff_id', 'job_id', 'employee_id', 'start_date', 'country_code', 'worker_id', 'office_code', 'job_code',
          'client_code', 'holiday_flag', 'work_hours', 'loading', 'end_date', 'term_flag', 'staff_name', 'job_title']


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
    for index, row in df.iterrows():
        try:
            query = f"INSERT INTO AEL.ODS_ADVISORY_TALENT_LINK ({inset_fields}) VALUES (%s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)"
            values = (row['staff_id'], row['job_id'], row['employee_id'], row['start_date'], row['country_code'],
                      row['worker_id'], row['office_code'], row['job_code'], row['client_code'], row['holiday_flag'],
                      row['work_hours'], row['loading'], row['end_date'], row['term_flag'], row['staff_name'],
                      row['job_title'])
            cursor.execute(query, values)
            connection.commit()
            print("数据插入成功")
        except Exception as e:
            print(e)
    cursor.close()
    connection.close()


if __name__ == '__main__':
    startDate = ''
    endDate = ''
    if startDate == '':
        startDate = current_date = datetime.now().date() - timedelta(days=5)
        endDate = datetime.now().date()
    table_name = "ods_advisory_talent_link"
    select_fields = "staff_id,job_id,employee_id,start_date,country_code,worker_id,office_code,job_code,client_code,holiday_flag,work_hours,loading,end_date,term_flag,staff_name,job_title"
    joint_index = "staff_id,job_id,employee_id,start_date"
    srcEngine = create_engine(
        f'mysql+pymysql://admin_user:{urllib.parse.quote_plus("6a!F@^ac*jBHtc7uUdxC")}@10.158.35.241:9030/advisory_engagement_lifecycle')
    print("任务开始执行")
    srcConn = srcEngine.connect()
    result = srcConn.execute(text(
        f"select count(*)as cnt from {table_name} where create_by_date >= \'{startDate}\' and create_by_date < \'{endDate}\'"))
    # 获取第一条元素的第一个字段
    data_count = result.fetchone()[0]
    print("doris数据量查询完成")
    gap = 100
    for i in range(0, data_count, gap):
        end = i + gap
        if end > data_count:
            end = data_count
        sql_main = text(
            f"select {select_fields} from (select ROW_NUMBER() OVER(Order by {joint_index}) AS rowNumber,* from {table_name} where create_by_date >= \'{startDate}\' and create_by_date < \'{endDate}\') as tbl where tbl.RowNumber >{i} and tbl.RowNumber <={end}")
        print("开始执行当前批次数据")
        run_new(sql_main, srcConn, select_fields)

    srcConn.close()
    print("数据同步完成")
