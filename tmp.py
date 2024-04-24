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
            "MONTH_END",
            "JOB_TERRITORY_CODE",
            "JOB_REGION",
            "CURRENT_JOB_LOS",
            "CURRENT_JOB_SUBLOS",
            "CURRENT_JOB_BU",
            "CURRENT_JOB_OU_CODE",
            "CURRENT_JOB_OU",
            "CLIENT_CODE",
            "CLIENT_NAME",
            "JOB_CODE",
            "JOB_OFFICE_CODE",
            "JOB_DESC",
            "CURRENT_JOB_PARTNER_STAFF_CODE",
            "CURRENT_JOB_PARTNER",
            "CURRENT_JOB_MANAGER_STAFF_CODE",
            "CURRENT_JOB_MANAGER",
            "CURRENT_BILL_PARTNER_STAFF_CODE",
            "CURRENT_BILL_PARTNER",
            "CURRENT_BILL_MANAGER_STAFF_CODE",
            "CURRENT_BILL_MANAGER",
            "CURRENT_DEBTOR_CODE",
            "CURRENT_DEBTOR_NAME",
            "IS_CNHK_INTER_TERRITORY_BILLING",
            "BILL_DATE",
            "BILL_NO_WITH_OFFICE_CODE",
            "BILL_AGING",
            "AR",
            "AR_PROVISION",
            "POTENTIAL_AR_PROVISION_IN_CURRENT_MONTH",
            "POTENTIAL_AR_PROVISION_IN_NEXT_MONTH",
            "POTENTIAL_AR_PROVISION_IN_THE_MONTH_AFTER_NEXT",
            "SD_ROW_CREATION",
            "ETL_TIME",
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
            if str(row["BILL_DATE"]) in ("NaT","None"):
                BILL_DATE_FIN = "NULL"
            else:
                BILL_DATE_FIN = '"' + str(row["BILL_DATE"]) + '"'
            query = f"""
			INSERT INTO AEL.DWD_FINACE_ONE_ADVISORY_OPERATIONS_DAY_EF (
				MONTH_END,JOB_TERRITORY_CODE,JOB_REGION,CURRENT_JOB_LOS,CURRENT_JOB_SUBLOS
                ,CURRENT_JOB_BU,CURRENT_JOB_OU_CODE,CURRENT_JOB_OU,CLIENT_CODE,CLIENT_NAME
                ,JOB_CODE,JOB_OFFICE_CODE,JOB_DESC,CURRENT_JOB_PARTNER_STAFF_CODE,CURRENT_JOB_PARTNER
                ,CURRENT_JOB_MANAGER_STAFF_CODE,CURRENT_JOB_MANAGER,CURRENT_BILL_PARTNER_STAFF_CODE,CURRENT_BILL_PARTNER,CURRENT_BILL_MANAGER_STAFF_CODE
                ,CURRENT_BILL_MANAGER,CURRENT_DEBTOR_CODE,CURRENT_DEBTOR_NAME,IS_CNHK_INTER_TERRITORY_BILLING,BILL_DATE
                ,BILL_NO_WITH_OFFICE_CODE,BILL_AGING,AR,AR_PROVISION,POTENTIAL_AR_PROVISION_IN_CURRENT_MONTH
                ,POTENTIAL_AR_PROVISION_IN_NEXT_MONTH,POTENTIAL_AR_PROVISION_IN_THE_MONTH_AFTER_NEXT,SD_ROW_CREATION,ETL_TIME
			)
			VALUES (
				"{row["MONTH_END"]}", "{row["JOB_TERRITORY_CODE"]}", "{row["JOB_REGION"]}", "{row["CURRENT_JOB_LOS"]}"
                , "{row["CURRENT_JOB_SUBLOS"]}", "{row["CURRENT_JOB_BU"]}", "{row["CURRENT_JOB_OU_CODE"]}", "{row["CURRENT_JOB_OU"]}", "{row["CLIENT_CODE"]}", "{row["CLIENT_NAME"]}"
				,"{row["JOB_CODE"]}", "{row["JOB_OFFICE_CODE"]}", "{row["JOB_DESC"]}", "{row["CURRENT_JOB_PARTNER_STAFF_CODE"]}", "{row["CURRENT_JOB_PARTNER"]}", "{row["CURRENT_JOB_MANAGER_STAFF_CODE"]}"
                , "{row["CURRENT_JOB_MANAGER"]}", "{row["CURRENT_BILL_PARTNER_STAFF_CODE"]}", "{row["CURRENT_BILL_PARTNER"]}", "{row["CURRENT_BILL_MANAGER_STAFF_CODE"]}"
				,"{row["CURRENT_BILL_MANAGER"]}", "{row["CURRENT_DEBTOR_CODE"]}", "{row["CURRENT_DEBTOR_NAME"]}", "{row["IS_CNHK_INTER_TERRITORY_BILLING"]}", {BILL_DATE_FIN}
                , "{row["BILL_NO_WITH_OFFICE_CODE"]}", "{row["BILL_AGING"]}", "{row["AR"]}", "{row["AR_PROVISION"]}", "{row["POTENTIAL_AR_PROVISION_IN_CURRENT_MONTH"]}"
				,"{row["POTENTIAL_AR_PROVISION_IN_NEXT_MONTH"]}", "{row["POTENTIAL_AR_PROVISION_IN_THE_MONTH_AFTER_NEXT"]}", "{row["SD_ROW_CREATION"]}", "{row["ETL_TIME"]}")
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
            f"""select count(1) cn from finance_bi.dwd_finace_one_advisory_operations_day_ef
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
					month_end as MONTH_END,
                    coalesce(trim(job_territory_code),'') as JOB_TERRITORY_CODE,
                    coalesce(trim(job_region),'') as JOB_REGION,
                    coalesce(trim(current_job_los),'') as CURRENT_JOB_LOS,
                    coalesce(trim(current_job_sublos),'') as CURRENT_JOB_SUBLOS,
                    coalesce(trim(current_job_bu),'') as CURRENT_JOB_BU,
                    coalesce(trim(current_job_ou_code),'') as CURRENT_JOB_OU_CODE,
                    coalesce(trim(current_job_ou),'') as CURRENT_JOB_OU,
                    coalesce(trim(client_code),'') as CLIENT_CODE,
                    coalesce(trim(replace(client_name,'"',"'")),'') as CLIENT_NAME,
                    coalesce(trim(job_code),'') as JOB_CODE,
                    coalesce(trim(job_office_code),'') as JOB_OFFICE_CODE,
                    coalesce(trim(replace(job_desc,'"',"'")),'') as JOB_DESC,
                    coalesce(trim(current_job_partner_staff_code),'') as CURRENT_JOB_PARTNER_STAFF_CODE,
                    coalesce(trim(current_job_partner),'') as CURRENT_JOB_PARTNER,
                    coalesce(trim(current_job_manager_staff_code),'') as CURRENT_JOB_MANAGER_STAFF_CODE,
                    coalesce(trim(current_job_manager),'') as CURRENT_JOB_MANAGER,
                    coalesce(trim(current_bill_partner_staff_code),'') as CURRENT_BILL_PARTNER_STAFF_CODE,
                    coalesce(trim(current_bill_partner),'') as CURRENT_BILL_PARTNER,
                    coalesce(trim(current_bill_manager_staff_code),'') as CURRENT_BILL_MANAGER_STAFF_CODE,
                    coalesce(trim(current_bill_manager),'') as CURRENT_BILL_MANAGER,
                    coalesce(trim(current_debtor_code),'') as CURRENT_DEBTOR_CODE,
                    coalesce(trim(replace(current_debtor_name,'"',"'")),'') as CURRENT_DEBTOR_NAME,
                    coalesce(trim(is_cnhk_inter_territory_billing),'') as IS_CNHK_INTER_TERRITORY_BILLING,
                    bill_date as BILL_DATE,
                    coalesce(trim(bill_no_with_office_code),'') as BILL_NO_WITH_OFFICE_CODE,
                    coalesce(bill_aging, 0) as BILL_AGING,
                    coalesce(trim(ar),'') as AR,
                    coalesce(trim(ar_provision),'') as AR_PROVISION,
                    coalesce(trim(potential_ar_provision_in_current_month),'') as POTENTIAL_AR_PROVISION_IN_CURRENT_MONTH,
                    coalesce(trim(potential_ar_provision_in_next_month),'') as POTENTIAL_AR_PROVISION_IN_NEXT_MONTH,
                    coalesce(trim(potential_ar_provision_in_the_month_after_next),'') as POTENTIAL_AR_PROVISION_IN_THE_MONTH_AFTER_NEXT,
                    sdrowcreation as SD_ROW_CREATION,
                    coalesce(trim(etl_time),'') as ETL_TIME
					from finance_bi.dwd_finace_one_advisory_operations_day_ef 
					order by MONTH_END,JOB_TERRITORY_CODE,JOB_REGION,CURRENT_JOB_LOS,CURRENT_JOB_SUBLOS
                ,CURRENT_JOB_BU,CURRENT_JOB_OU_CODE,CURRENT_JOB_OU,CLIENT_CODE,CLIENT_NAME
                ,JOB_CODE,JOB_OFFICE_CODE,JOB_DESC,CURRENT_JOB_PARTNER_STAFF_CODE,CURRENT_JOB_PARTNER
                ,CURRENT_JOB_MANAGER_STAFF_CODE,CURRENT_JOB_MANAGER,CURRENT_BILL_PARTNER_STAFF_CODE,CURRENT_BILL_PARTNER,CURRENT_BILL_MANAGER_STAFF_CODE
					 limit {i},5000
			;"""
        )
        print(f"开始执行当前批次数据  {i}")
        run(sql_main, srcConn)
        time.sleep(10)
    srcConn.close()
    srcEngine.dispose()
    print("数据同步完成")
