import time
import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus as urlquote

CURR_DATE = datetime.datetime.now().date()
CURR_DATE_STR = f"'{CURR_DATE}'"
SELECT_DEPENDENCE_DATE_SQL = f"""
select IF(sum(a)=7,1,0)as r from(
    select IF(date(max(sd_row_creation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_fact_job_hours_day_ef
    union all select IF(date(max(sd_row_creation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_calendar_day_ef
    union all select IF(date(max(sdrowcreation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_current_job_day_ei
    union all select IF(date(max(sdrowcreation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_current_client_day_ei
    union all select IF(date(max(sd_row_creation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_firm_structure_day_ef
    union all select IF(date(max(sdrowcreation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_current_staff_day_ei
    union all select IF(date(max(sd_row_creation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_staff_day_ef)as r
"""

SELECT_TARGET_MIN_DATE = """
select min(max_time)as min_time from(
select max(sd_row_creation) as max_time from finance_bi.ods_finance_dw_tbl_fact_current_cash_day_ef  union all
select max(sdrowcreation) as max_time from finance_bi.ods_finance_dw_tbl_dim_current_job_day_ei  union all
select max(sdrowcreation) as max_time from finance_bi.ods_finance_dw_tbl_dim_current_client_day_ei  union all
select max(sd_row_creation) as max_time from finance_bi.ods_finance_dw_tbl_dim_firm_structure_day_ef  union all
select max(sdrowcreation) as max_time from finance_bi.ods_finance_dw_tbl_dim_current_bill_day_ei  union all
select max(sd_row_creation) as max_time from finance_bi.ods_finance_dw_tbl_dim_calendar_day_ef  union all
select max(sdrowcreation) as max_time from finance_bi.ods_finance_dw_tbl_dim_current_staff_day_ei  union all
select max(sd_row_creation) as max_time from finance_bi.ods_finance_dw_tbl_fact_current_ar_journal_day_ef)as r
"""
INSERT_TARGET_SQL = """
insert into finance_bi.dwd_finance_ael_collection_data_day_ef(client_name, client_code, bill_no,bill_office_code,bill_office_code_with_bill_no, bill_date,
                                                              cash_receipt_date, cash_process_date, job_code, job_desc,
                                                              current_job_charge_code, job_los, job_sub_los, job_bu,
                                                              job_ou, job_ou_code, job_territory_code, job_region,
                                                              job_office_code, job_partner_staff_code, job_partner,
                                                              job_manager_staff_code, job_manager,
                                                              current_bill_partner_staff_code, current_bill_partner,
                                                              current_bill_manager_staff_code, current_bill_manager,
                                                              current_debtor_code, current_debtor_name,
                                                              cash_collection_amount, sd_row_creation, etl_date)
WITH APC
         AS (
        SELECT CONCAT('CN-', int_assgnmt_id) AS current_job_key,
               sd_per_end_date,
               int_assgnmt_id,
               ch_asg_ptr_staff_code,
               ch_asg_mgr_staff_code
        FROM pwc_mdm.ods_ipower_tbl_assg_period_close_cn_day_ei
        where left(sd_per_end_date, 4) >=
              (SELECT MAX(fiscal_year_key) - 2 FROM finance_bi.ods_finance_dw_tbl_fact_current_cash_day_ef)
        UNION ALL
        SELECT CONCAT('HK-', int_assgnmt_id) AS current_job_key,
               sd_per_end_date,
               int_assgnmt_id,
               ch_asg_ptr_staff_code,
               ch_asg_mgr_staff_code
        FROM pwc_mdm.ods_ipower_tbl_assg_period_close_hk_day_ei
        where left(sd_per_end_date, 4) >=
              (SELECT MAX(fiscal_year_key) - 2 FROM finance_bi.ods_finance_dw_tbl_fact_current_cash_day_ef)
    ),
     Cash
         AS (
         SELECT CC.client_name,
                Cash.client_key                      AS client_code,
                CB.bill_no,
                CB.bill_office_code,
                Cash.bill_date_key                   AS bill_date,
                Cash.receipt_date_key                AS cash_receipt_date,
                Cash.date_key                        AS cash_process_date,
                CJ.job_code,
                CJ.job_desc,
                CJ.current_job_charge_code,
                JobFS.los                            AS job_los,
                JobFS.sub_los                        AS job_sub_los,
                JobFS.bu                             AS job_bu,
                JobFS.ou                             AS job_ou,
                JobFS.ou_code                        AS job_ou_code,
                CJ.job_territory_code,
                CJ.job_region,
                CJ.job_office_code,
                JP.regional_staff_code               as job_partner_staff_code,
                JP.staff_name                        as job_partner,
                JM.regional_staff_code               as job_manager_staff_code,
                JM.staff_name                        as job_manager,
                CB.current_debtor_partner_staff_code as current_bill_partner_staff_code,
                CB.current_debtor_partner            as current_bill_partner,
                CB.current_debtor_manager_staff_code as current_bill_manager_staff_code,
                CB.current_debtor_manager            as current_bill_manager,
                CB.current_debtor_code,
                CB.current_debtor_name,
                Cash.cash_receipt,
                Cash.sd_row_creation
         FROM (select *
               from finance_bi.ods_finance_dw_tbl_fact_current_cash_day_ef
               where fiscal_year_key >=
                     (SELECT MAX(fiscal_year_key) - 2 FROM finance_bi.ods_finance_dw_tbl_fact_current_cash_day_ef)
                 AND NOT (cash_receipt = 0)
              ) AS Cash
                  INNER JOIN (select job_code,
                                     job_desc,
                                     current_job_charge_code,
                                     job_territory_code,
                                     job_region,
                                     job_office_code,
                                     current_job_key
                              from finance_bi.ods_finance_dw_tbl_dim_current_job_day_ei
                              where current_job_charge_code IN ('EB', 'IB')) AS CJ
                             ON Cash.current_job_key = CJ.current_job_key
                  INNER JOIN (select client_code, client_name
                              from finance_bi.ods_finance_dw_tbl_dim_current_client_day_ei) AS CC
                             ON Cash.client_key = CC.client_code
                  INNER JOIN (select *
                              from finance_bi.ods_finance_dw_tbl_dim_firm_structure_day_ef
                              where los_code = 'ADV  '
                                 OR (los_code = 'OFS  ' AND sub_los_code like '%48%')
         ) AS JobFS
                             ON Cash.cy_job_ou_key = JobFS.ou_key
                  LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_current_bill_day_ei AS CB
                            ON Cash.current_bill_key = CB._CurrentBillKey
                  LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_calendar_day_ef AS Cal
                            ON Cash.date_key = Cal.date_key
                  LEFT JOIN APC AS APC
                            ON Cal.period_end = APC.sd_per_end_date
                                AND Cash.current_job_key = APC.current_job_key
                  LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_current_staff_day_ei AS JP
                            ON APC.ch_asg_ptr_staff_code = JP.staff_code
                  LEFT JOIN (select staff_code, regional_staff_code, staff_name
                             from finance_bi.ods_finance_dw_tbl_dim_current_staff_day_ei) AS JM
                            ON APC.ch_asg_mgr_staff_code = JM.staff_code
     ),
     ARJournal
         AS (
         SELECT CC.client_name,
                ARJ.client_key                       AS client_code,
                CB.bill_no,
                CB.bill_office_code,
                ARJ.bill_date_key                    AS bill_date,
                ARJ.date_key                         AS ar_journal_process_date,
                CJ.job_code,
                CJ.job_desc,
                CJ.current_job_charge_code,
                JobFS.los                            AS job_los,
                JobFS.sub_los                        AS job_sub_los,
                JobFS.bu                             AS job_bu,
                JobFS.ou                             AS job_ou,
                JobFS.ou_code                        AS job_ou_code,
                CJ.job_territory_code,
                CJ.job_region,
                CJ.job_office_code,
                JP.regional_staff_code               AS job_partner_staff_code,
                JP.staff_name                        AS job_partner,
                JM.regional_staff_code               AS job_manager_staff_code,
                JM.staff_name                        AS job_manager,
                CB.current_debtor_partner_staff_code AS current_bill_partner_staff_code,
                CB.current_debtor_partner            AS current_bill_partner,
                CB.current_debtor_manager_staff_code As current_bill_manager_staff_code,
                CB.current_debtor_manager            AS current_bill_manager,
                CB.current_debtor_code,
                CB.current_debtor_name,
                ARJ.ar_journal_amount,
                ARJ.sd_row_creation
         FROM (select *
               from finance_bi.ods_finance_dw_tbl_fact_current_ar_journal_day_ef
               where fiscal_year_key >=
                     (SELECT MAX(fiscal_year_key) - 2 FROM finance_bi.ods_finance_dw_tbl_fact_current_ar_journal_day_ef)
                 AND NOT (
                   ar_journal_amount = 0
                   )) AS ARJ
                  INNER JOIN (select *
                              from finance_bi.ods_finance_dw_tbl_dim_current_job_day_ei
                              where current_job_charge_code IN ('EB', 'IB')) AS CJ
                             ON ARJ.current_job_key = CJ.current_job_key
                  INNER JOIN finance_bi.ods_finance_dw_tbl_dim_current_client_day_ei AS CC
                             ON ARJ.client_key = CC.client_code
                  INNER JOIN (select *
                              from finance_bi.ods_finance_dw_tbl_dim_firm_structure_day_ef
                              where los_code = 'ADV  '
                                 OR (los_code = 'OFS  ' AND sub_los_code like '%48%')) AS JobFS
                             ON ARJ.cy_job_ou_key = JobFS.ou_key
                  LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_current_bill_day_ei AS CB
                            ON ARJ.current_bill_key = CB._CurrentBillKey
                  LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_calendar_day_ef AS Cal
                            ON ARJ.date_key = Cal.date_key
                  LEFT JOIN APC AS APC
                            ON Cal.period_end = APC.sd_per_end_date
                                AND ARJ.current_job_key = APC.current_job_key
                  LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_current_staff_day_ei AS JP
                            ON APC.ch_asg_ptr_staff_code = JP.staff_code
                  LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_current_staff_day_ei AS JM
                            ON APC.ch_asg_mgr_staff_code = JM.staff_code
     )

SELECT client_name,
       client_code,
       bill_no,
       bill_office_code,
       concat(trim(bill_office_code),trim(bill_no))as bill_office_code_with_bill_no,
       bill_date,
       cash_receipt_date,
       cash_process_date,
       job_code,
       job_desc,
       current_job_charge_code,
       job_los,
       job_sub_los,
       job_bu,
       job_ou,
       job_ou_code,
       job_territory_code,
       job_region,
       job_office_code,
       job_partner_staff_code,
       job_partner,
       job_manager_staff_code,
       job_manager,
       current_bill_partner_staff_code,
       current_bill_partner,
       current_bill_manager_staff_code,
       current_bill_manager,
       current_debtor_code,
       current_debtor_name,
       cash_receipt AS cash_collection_amount,
       '{}' as sd_row_creation,
       curdate() as etl_date
FROM Cash
UNION
SELECT client_name,
       client_code,
       bill_no,
       bill_office_code,
       concat(trim(bill_office_code),trim(bill_no))as bill_office_code_with_bill_no,
       bill_date,
       NULL                    AS cash_receipt_date,
       ar_journal_process_date AS cash_process_date,
       job_code,
       job_desc,
       current_job_charge_code,
       job_los,
       job_sub_los,
       job_bu,
       job_ou,
       job_ou_code,
       job_territory_Code,
       job_region,
       job_office_code,
       job_partner_staff_code,
       job_partner,
       job_manager_staff_code,
       job_manager,
       current_bill_partner_staff_code,
       current_bill_partner,
       current_bill_manager_staff_code,
       current_bill_manager,
       current_debtor_code,
       current_debtor_name,
       ar_journal_amount       AS cash_collection_amount,
       '{}' as sd_row_creation,
       curdate() as etl_date
FROM ARJournal;
"""


def truncateTable(engine, table_name):
    tarConn = engine.connect()
    tarConn.execute(text(f"truncate table {table_name}"))
    tarConn.close()
    print("table truncate is complete")


def generate_target_table(table_name):
    engine = create_engine(
        f'mysql+pymysql://admin_user:{urlquote("6a!F@^ac*jBHtc7uUdxC")}@10.158.15.148:6030'
        f'/finance_bi')
    conn = engine.connect()
    cursor = conn.connection.cursor()
    r = cursor.execute(SELECT_DEPENDENCE_DATE_SQL)
    if r == 1:
        print("依赖表数据日期为最新日期，开始执行")
        truncateTable(engine, table_name)
        min_time = cursor.execute(SELECT_TARGET_MIN_DATE)
        cursor.execute(INSERT_TARGET_SQL.format(min_time, min_time))
        print("sql执行成功")
        conn.commit()
        conn.close()
        engine.dispose()
    else:
        if CURR_DATE == datetime.datetime.now().date():
            print(f"依赖表有数据未更新，等待依赖表数据更新，当前时间为{datetime.datetime.now()}")
            conn.close()
            engine.dispose()
            time.sleep(1800)
            generate_target_table(table_name)


if __name__ == '__main__':
    targetTableName = "dwd_finance_ael_collection_data_day_ef"
    srcEngine = create_engine(
        f'mysql+pymysql://admin_user:{urlquote("6a!F@^ac*jBHtc7uUdxC")}@10.158.15.148:6030'
        f'/finance_bi')
    srcConn = srcEngine.connect()
    result_1 = srcConn.execute(text(
        f"select IF(date(sd_row_creation) = {CURR_DATE}, 1, 0)as r from {targetTableName} limit 1"))
    # 获取第一条元素的第一个字段
    newestDateFlag = result_1.fetchone()[0]
    srcConn.close()
    if newestDateFlag == 0:
        print("数据不是最新日期，任务开始执行")
        generate_target_table(targetTableName)
    else:
        print("数据日期为最新日期，无需执行")
