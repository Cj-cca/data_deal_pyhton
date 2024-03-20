import pymysql
import pandas as pd
import numpy as np
import sys
import calendar
from datetime import datetime
import time
import pymssql
import pyodbc
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus as urlquote
import warnings

warnings.filterwarnings("ignore")

ETL_TIME = "${batch_date_ymd} ${batch_date_hms}"


def run(sql_main, srcConn):
    print("开始创建连接")
    connection = pymysql.connect(
        host="10.157.112.167",
        port=3306,
        user="oats_talentlink",
        password="Fo@tI%Vwc(AO",
        db="AEL",
        charset="utf8",
    )
    cursor = connection.cursor()
    print("连接创建完成")
    pd_src = pd.read_sql(sql_main, srcConn)
    print("数据读取成功，开始同步数据")
    pd_src["ETL_TIME"] = ETL_TIME
    pd_src[
        [
            "JOB_TERRITORY_CODE",
            "CURRENT_JOB_LOS",
            "CURRENT_JOB_SUBLOS",
            "CURRENT_JOB_BU",
            "CURRENT_JOB_OU_CODE",
            "CURRENT_JOB_OU",
            "JOB_OFFICE",
            "CLIENT_CODE",
            "CLIENT_NAME",
            "JOB_CODE",
            "JOB_DESC",
            "CURRENT_JOB_PARTNER_STAFF_CODE",
            "CURRENT_JOB_PARTNER",
            "CURRENT_JOB_MANAGER_STAFF_CODE",
            "CURRENT_JOB_MANAGER",
            "CREATE_DATE",
            "CURRENT_JOB_CHARGE_CODE",
            "JOB_TERMINATION_DATE",
            "TOTAL_ESTIMATED_FEE",
            "NET_ESTIMATED_FEE",
            "CURRENT_JOB_LATEST_BUDGET_RATE",
            "ETL_TIME",
            "THIS_JOB_PIC_SIGNED_THE_EL",
            "OPPORTUNITY_ID"
        ]
    ].replace([pd.NA, np.nan], None, inplace=True)
    # pd_src["BILL_DATE"] = pd_src["BILL_DATE"].dt.strftime("%Y-%m-%d %H:%M:%S")
    # pd_src["BILL_DATE"] = pd_src["BILL_DATE"].fillna(pd.NaT)
    # print(pd_src[["BILL_DATE"]])
    # sys.exit()
    print("read table success")
    try:
        for index, row in pd_src.iterrows():
            # oppid = row["OPP_ID"]
            # if row["OPP_ID"] != row["OPP_ID"]:
            #     oppid = None
            if str(row["CREATE_DATE"]) in ("NaT", "None"):
                CREATE_DATE_FIN = "NULL"
            else:
                CREATE_DATE_FIN = '"' + str(row["CREATE_DATE"]) + '"'
            if str(row["JOB_TERMINATION_DATE"]) in ("NaT", "None"):
                JOB_TERMINATION_DATE_FIN = "NULL"
            else:
                JOB_TERMINATION_DATE_FIN = '"' + str(row["JOB_TERMINATION_DATE"]) + '"'
            if str(row["JOB_DESC"]).find('"') != -1:
                JOB_DESC_FIN = "'" + str(row["JOB_DESC"]) + "'"
            else:
                JOB_DESC_FIN = '"' + str(row["JOB_DESC"]) + '"'
            if str(row["CLIENT_NAME"]).find('"') != -1:
                CLIENT_NAME_FIN = "'" + str(row["CLIENT_NAME"]) + "'"
            else:
                CLIENT_NAME_FIN = '"' + str(row["CLIENT_NAME"]) + '"'
            query = f"""
			INSERT INTO AEL.DWD_FINANCE_NEW_PROJECT_DATA_DAY_EF (
				JOB_TERRITORY_CODE,CURRENT_JOB_LOS,CURRENT_JOB_SUBLOS,CURRENT_JOB_BU,CURRENT_JOB_OU_CODE,CURRENT_JOB_OU,JOB_OFFICE,CLIENT_CODE
                ,CLIENT_NAME,JOB_CODE,JOB_DESC,CURRENT_JOB_PARTNER_STAFF_CODE,CURRENT_JOB_PARTNER,CURRENT_JOB_MANAGER_STAFF_CODE,CURRENT_JOB_MANAGER
                ,CREATE_DATE,CURRENT_JOB_CHARGE_CODE,JOB_TERMINATION_DATE,TOTAL_ESTIMATED_FEE,NET_ESTIMATED_FEE,CURRENT_JOB_LATEST_BUDGET_RATE
                ,ETL_TIME,THIS_JOB_PIC_SIGNED_THE_EL,OPPORTUNITY_ID
			)
			VALUES ("{row["JOB_TERRITORY_CODE"]}",
                    "{row["CURRENT_JOB_LOS"]}",
                    "{row["CURRENT_JOB_SUBLOS"]}",
                    "{row["CURRENT_JOB_BU"]}",
                    "{row["CURRENT_JOB_OU_CODE"]}",
                    "{row["CURRENT_JOB_OU"]}",
                    "{row["JOB_OFFICE"]}",
                    "{row["CLIENT_CODE"]}",
                    {CLIENT_NAME_FIN},
                    "{row["JOB_CODE"]}",
                    {JOB_DESC_FIN},
                    "{row["CURRENT_JOB_PARTNER_STAFF_CODE"]}",
                    "{row["CURRENT_JOB_PARTNER"]}",
                    "{row["CURRENT_JOB_MANAGER_STAFF_CODE"]}",
                    "{row["CURRENT_JOB_MANAGER"]}",
                    {CREATE_DATE_FIN},
                    "{row["CURRENT_JOB_CHARGE_CODE"]}",
                    {JOB_TERMINATION_DATE_FIN},
                    "{row["TOTAL_ESTIMATED_FEE"]}",
                    "{row["NET_ESTIMATED_FEE"]}",
                    "{row["CURRENT_JOB_LATEST_BUDGET_RATE"]}",
                    "{row["ETL_TIME"]}",
                    "{row["THIS_JOB_PIC_SIGNED_THE_EL"]}",
                    "{row["OPPORTUNITY_ID"]}")
			"""
            # print(query)
            # sys.exit()
            cursor.execute(query)
    except Exception as e:
        print(e, "   ", row["JOB_TERRITORY_CODE"])
        print(query)
    finally:
        connection.commit()
        print("数据插入成功")
        cursor.close()
        connection.close()


if __name__ == "__main__":
    srcEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.15.148:6030/finance_bi"
    )
    print("任务开始执行")
    srcConn = srcEngine.connect()
    result = srcConn.execute(
        text(
            f"""select count(1) cn from finance_bi.dwd_finance_new_project_data_day_ef
            """
        )
    )
    # 获取第一条元素的第一个字段
    data_count = result.fetchone()[0]
    print(data_count)
    # data_count = 100
    gap = 5000
    for i in range(0, data_count, gap):
        sql_main = text(
            f"""select 
                    job_territory_code as JOB_TERRITORY_CODE,
                    current_job_los as CURRENT_JOB_LOS,
                    current_job_sublos as CURRENT_JOB_SUBLOS,
                    current_job_bu as CURRENT_JOB_BU,
                    current_job_ou_code as CURRENT_JOB_OU_CODE,
                    current_job_ou as CURRENT_JOB_OU,
                    job_office as JOB_OFFICE,
                    client_code as CLIENT_CODE,
                    client_name as CLIENT_NAME,
                    job_code as JOB_CODE,
                    job_desc as JOB_DESC,
                    current_job_partner_staff_code as CURRENT_JOB_PARTNER_STAFF_CODE,
                    current_job_partner as CURRENT_JOB_PARTNER,
                    current_job_manager_staff_code as CURRENT_JOB_MANAGER_STAFF_CODE,
                    current_job_manager as CURRENT_JOB_MANAGER,
                    create_date as CREATE_DATE,
                    current_job_charge_code as CURRENT_JOB_CHARGE_CODE,
                    job_termination_date as JOB_TERMINATION_DATE,
                    total_estimated_fee as TOTAL_ESTIMATED_FEE,
                    net_estimated_fee as NET_ESTIMATED_FEE,
                    current_job_latest_budget_rate as CURRENT_JOB_LATEST_BUDGET_RATE,
                    etl_time as ETL_TIME,
                    this_job_pic_signed_the_el as THIS_JOB_PIC_SIGNED_THE_EL,
                    opportunity_id as OPPORTUNITY_ID
                from finance_bi.dwd_finance_new_project_data_day_ef 
                order by job_territory_code,current_job_los,current_job_sublos,current_job_bu,current_job_ou_code,current_job_ou,job_office,client_code
                    limit {i},5000
			;"""
        )
        print(f"开始执行当前批次数据  {i}")
        run(sql_main, srcConn)
        time.sleep(10)
    srcConn.close()
    srcEngine.dispose()
    print("数据同步完成")
