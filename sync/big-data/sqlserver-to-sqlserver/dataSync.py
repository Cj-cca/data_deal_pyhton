# !/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from enum import Enum
import datetime
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote_plus as urlquote
import queue

FieldMapping = {"intAssgTimeDtlID": "int_assg_time_dtl_id",
                "intAssgnmtID": "int_assgnmt_id",
                "chClientCode": "ch_client_code",
                "chJobCode": "ch_job_code",
                "chAsgOfficeCode": "ch_asg_office_code",
                "chAsgGroupCode": "ch_asg_group_code",
                "sdPerEndDate": "sd_per_end_date",
                "sdPerEntryDate": "sd_per_entry_date",
                "sdProcessDate": "sd_process_date",
                "chProjectCode": "ch_project_code",
                "chYear": "ch_year",
                "chTimeJnlType": "ch_time_jnl_type",
                "sdDailyDate": "sd_daily_date",
                "sintLineNo": "sint_line_no",
                "chOrigOfficeCode": "ch_orig_office_code",
                "chVoucherNo": "ch_voucher_no",
                "chJnlNo": "ch_jnl_no",
                "dcHours": "dc_hours",
                "dcTargetRate": "dc_target_rate",
                "dcWipTargetAmt": "dc_wip_target_amt",
                "dcBudgetRate": "dc_budget_rate",
                "dcWIPBudgetAmt": "dc_wip_budget_amt",
                "dcActTimeCostAmt": "dc_act_time_cost_amt",
                "dcActOthCostAmt": "dc_act_oth_cost_amt",
                "chChargeType": "ch_charge_type",
                "chStaffCode": "ch_staff_code",
                "chStaffOfficeCode": "ch_staff_office_code",
                "chStaffGroupCode": "ch_staff_group_code",
                "chStaffGradeCode": "ch_staff_grade_code",
                "chStaffEntityCode": "ch_staff_entity_code",
                "tintStaffIndex": "tint_staff_index",
                "chFinalBilledFlag": "ch_final_billed_flag",
                "intGenExpId": "int_gen_exp_id",
                "intAssgBudgetRateID": "int_assg_budget_rate_id",
                "dtUpdateDateTime": "dt_update_date_time",
                "intFinJnlDtlID": "int_fin_jnl_dtl_id",
                "intTimeSheetHoursID": "int_time_sheet_hours_id",
                "dcStdTimeCostAmt": "dc_std_time_cost_amt",
                "dcStdOthCostAmt": "dc_std_oth_cost_amt",
                "dcRevisedBillAmt": "dc_revised_bill_amt",
                "dcRevisedBillHrs": "dc_revised_bill_hrs",
                "dtoLastTaggedDatetime": "dto_last_tagged_datetime",
                "chLastTaggedStaffCode": "ch_last_tagged_staff_code",
                "nvcWriteOffComment": "nvc_write_off_comment",
                "chRegionCode": "ch_region_code",
                "chWIPTaggingStatusCode": "ch_wip_tagging_status_code",
                "intFeeAllocationID": "int_fee_allocation_id"}


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
    insert_count = -1
    try:
        pdDataFrame = pd.read_sql(sql, srcConn)
        print("读取数据成功")
        pdDataFrame.rename(columns=FieldMapping, inplace=True)
        pdDataFrame['etl_time'] = datetime.datetime.now().date()
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
    with MyThreadPoolExecutor(max_workers=20) as t:
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
    srcTable = "PwCMDM.dbo.tblAssgTimeDtl_CN"
    tarTable = "ods_ipower_tbl_assg_time_dtl_cn_day_ei"
    # srcEngine = getEngine("CNCSQLPWV5028", "App_Airflow", "P@ss1234567890", 1800, EngineType.sqlserver, "PwCMDM")
    srcEngine = getEngine("CNCSQLPWV5028", "App_AirFlow", "P@ss1234567890", 1800, EngineType.sqlserver, "PwCMDM")
    tarEngine = getEngine("10.158.15.148", "admin_user", "6a!F@^ac*jBHtc7uUdxC", 6030, EngineType.mysql, "pwc_mdm")
    uniqueIndex = "intAssgTimeDtlID"
    selectField = ("intAssgTimeDtlID,intAssgnmtID,chClientCode,chJobCode,chAsgOfficeCode,chAsgGroupCode,sdPerEndDate,"
                   "sdPerEntryDate,sdProcessDate,chProjectCode,chYear,chTimeJnlType,sdDailyDate,sintLineNo,"
                   "chOrigOfficeCode,chVoucherNo,chJnlNo,dcHours,dcTargetRate,dcWipTargetAmt,dcBudgetRate,"
                   "dcWIPBudgetAmt,dcActTimeCostAmt,dcActOthCostAmt,chChargeType,chStaffCode,chStaffOfficeCode,"
                   "chStaffGroupCode,chStaffGradeCode,chStaffEntityCode,tintStaffIndex,chFinalBilledFlag,intGenExpId,"
                   "intAssgBudgetRateID,dtUpdateDateTime,intFinJnlDtlID,intTimeSheetHoursID,dcStdTimeCostAmt,"
                   "dcStdOthCostAmt,dcRevisedBillAmt,dcRevisedBillHrs,dtoLastTaggedDatetime,chLastTaggedStaffCode,"
                   "nvcWriteOffComment,chRegionCode,chWIPTaggingStatusCode,intFeeAllocationID")
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
    runSyncDataWithIndex()
