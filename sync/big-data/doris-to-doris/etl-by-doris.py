import pymysql
import pandas as pd
import calendar
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from sqlalchemy import create_engine
import warnings

warnings.filterwarnings("ignore")

# start:23200130,end:79584359
def run(conn_pro, conn_uat,tableName):
    sql_tm = f"""select * from {tableName}
                where sdDailyDate >= DATE_SUB(now() ,INTERVAL 25 MONTH) and sdDailyDate < DATE_SUB(now() ,INTERVAL 20 MONTH)"""
    pd_src = pd.read_sql(sql_tm, conn_pro)
    print(tableName, "数据读取成功，开始写入数据")
    pd_src.to_sql(tableName, conn_uat, if_exists='append', index=False)
    print("数据写入完成")
    # pd_src = pd.read_sql(f"select * from {tableName} where intTimeSheetMemoID>45590184 limit 10", conn_pro)
    # pd_src.size
    # print(tableName, "数据读取成功，开始写入数据")
    # pd_src.to_sql(tableName, conn_uat, if_exists='append', index=False)
    # print("数据写入完成")
    # gap = 5
    # for i in range(5000000, 10000000000, gap):
    #     # sql_tm = f"""select * from {tableName}
    #     # where intAssgTimeDtlID >= {i} and intAssgTimeDtlID<{i+gap}"""
    #     sql_tm = f"""select * from {tableName}
    #         where sdDailyDate > DATE_SUB(CURRENT_DATE() ,INTERVAL 25 MONTH) and sdDailyDate > DATE_SUB(CURRENT_DATE() ,INTERVAL 25 MONTH)"""
    #     pd_src = pd.read_sql(sql_tm, conn_pro)
    #     if pd_src.size == 0:
    #         print("数据查询完成")
    #         break
    #     else:
    #         print("数据读取成功，开始写入数据,数据条数：", pd_src.size)
    #         pd_src.to_sql(tableName, conn_uat, if_exists='append', index=False)
    #         print("数据写入成功")

def run1(conn_pro, conn_uat,tableName):
    # pd_src = pd.read_sql(f"select * from {tableName} where intTimeSheetMemoID>45590184 limit 10", conn_pro)
    # pd_src.size
    # print(tableName, "数据读取成功，开始写入数据")
    # pd_src.to_sql(tableName, conn_uat, if_exists='append', index=False)
    # print("数据写入完成")
    gap = 10000
    with ThreadPoolExecutor(max_workers=8) as t:
        taskList = []
        for i in range(23200131, 79584359, gap):
            start = i
            end = i + gap
            if end > 79584359:
                end = 79584359
            sql_tm = f"""select * from {tableName}
            where intAssgTimeDtlID >= {start} and intAssgTimeDtlID<{end}"""
            pd_src = pd.read_sql(sql_tm, conn_pro)
            if pd_src.size == 0:
                print("数据查询完成")
                break
            else:
                obj = t.submit(writeData, (pd_src, tableName))
                taskList.append(obj)
                # pd_src.to_sql(tableName, conn_uat, if_exists='append', index=False)
                # print("数据写入成功")

        # for future in as_completed(taskList):
        #     data = future.result()
        #     print(f"数据写入成功,数据条数: {data}")

def writeData(arg):
    pd_sink = arg[0]
    tableName = arg[1]
    conn = create_engine('mysql+pymysql://root@10.158.16.244:9030/ChinaPower')
    print("数据读取成功，开始写入数据,数据条数：", pd_sink.size)
    count = pd_sink.to_sql(tableName, conn, if_exists='append', index=False)
    print("数据写入成功,数据条数：", count)



if __name__ == '__main__':
    tableNameList = "Opportunity_tblOpportunity_new,Core_tblProduct_new,Core_tblClient_new,Core_tblStaff_new,Client_tblJob_new,dbo_tblAssgProject_new".split(",")
    #生产环境
    doris_pro = create_engine('mysql+pymysql://root@10.158.34.175:9030/ChinaPower')
    proConn = pymysql.connect(
        host='10.158.34.175',
        port=9030,
        user='root',
        password='',
        database='ChinaPower',
        charset='utf8'
    )
    #预发布环境
    doris_uat = create_engine('mysql+pymysql://root@10.158.16.244:9030/ChinaPower')
    st = time.time()
    # for tableName in tableNameList:
    #     run(proConn, doris_uat, tableName)
    run1(proConn, doris_uat, "dbo_tblAssgTimeDtl_new")
    print('time : ', time.time() - st)