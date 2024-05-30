import time
import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus as urlquote

CURR_DATE = datetime.datetime.now().date()
CURR_DATE_STR = f"'{CURR_DATE}'"
SELECT_DEPENDENCE_DATE_SQL = f"""
select IF(sum(a)=6,1,0)as r from(
select IF(date(max(sdrowcreation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_current_job_day_ei
union all select IF(date(max(sdrowcreation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_current_client_day_ei
union all select IF(date(max(sd_row_creation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_firm_structure_day_ef
union all select IF(date(max(sd_row_creation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_calendar_day_ef
union all select IF(date(max(sdrowcreation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_current_staff_day_ei
union all select IF(date(max(sd_row_creation)) = {CURR_DATE_STR}, 1, 0)as a from finance_bi.ods_finance_dw_tbl_dim_exchange_rate_day_ef)as r
"""

SELECT_TARGET_MIN_DATE = """
select min(max_time)as min_time from(
select max(sdrowcreation) as max_time from finance_bi.ods_finance_dw_tbl_dim_current_job_day_ei
union all select max(sdrowcreation) as max_time from finance_bi.ods_finance_dw_tbl_dim_current_client_day_ei
union all select max(sd_row_creation) as max_time from finance_bi.ods_finance_dw_tbl_dim_firm_structure_day_ef
union all select max(sd_row_creation) as max_time from finance_bi.ods_finance_dw_tbl_dim_calendar_day_ef
union all select max(sdrowcreation) as max_time from finance_bi.ods_finance_dw_tbl_dim_current_staff_day_ei
union all select max(sd_row_creation) as max_time from finance_bi.ods_finance_dw_tbl_dim_exchange_rate_day_ef
)as r
"""

INSERT_TARGET_SQL = """
insert into finance_bi.dwd_finance_revenue_data_day_ef(job_code, client_code, fiscal_year_name, month_end, job_partner, job_partner_staff_code, job_partner_staff_ou_code, uhc_name, uhc_prid, client_name, job_desc, job_office_code, product, job_ou_code, job_los, hours_in_revenue, revenue_in_hkd, em_in_hkd, gross_target_value_in_hkd, sd_row_creation, etl_time)
WITH APC
AS (
    SELECT CONCAT('CN-', int_assgnmt_id) AS current_job_key,
               sd_per_end_date,
               int_assgnmt_id,
               ch_asg_ptr_staff_code,
               ch_asg_mgr_staff_code
    FROM pwc_mdm.ods_ipower_tbl_assg_period_close_cn_day_ei
    where sd_per_end_date >= (select min(date_key) from finance_bi.ods_finance_dw_tbl_fact_revenue_day_ef
    where fiscal_year_key >= (SELECT MAX(fiscal_year_key) FROM finance_bi.ods_finance_dw_tbl_fact_revenue_day_ef)-1)
    UNION ALL
    SELECT CONCAT('HK-', int_assgnmt_id) AS current_job_key,
               sd_per_end_date,
               int_assgnmt_id,
               ch_asg_ptr_staff_code,
               ch_asg_mgr_staff_code
        FROM pwc_mdm.ods_ipower_tbl_assg_period_close_hk_day_ei
    where sd_per_end_date >= (select min(date_key) from finance_bi.ods_finance_dw_tbl_fact_revenue_day_ef
    where fiscal_year_key >= (SELECT MAX(fiscal_year_key) FROM finance_bi.ods_finance_dw_tbl_fact_revenue_day_ef)-1)
)
SELECT
    CJ.job_code,
    Rev.client_key AS client_code,
    Cal.fiscal_year_name,
    Cal.month_end,
    JP.staff_name AS job_partner,
    JP.regional_staff_code AS job_partner_staff_code,
    JP.regional_staff_ou_code AS job_partner_staff_ou_code,
    CC.uhc_name,
    CC.uhc_prid,
    CC.client_name,
    CJ.job_desc,
    CJ.job_office_code,
    CJ.product,
    JobFS.ou_code AS job_ou_code,
    JobFS.los AS job_los,
    SUM(Rev.hours_in_revenue) AS hours_in_revenue,
    SUM(round(Rev.revenue,13) * round(HKD.reporting_exchange_rate,13)) AS revenue_in_hkd,
    SUM(round(Rev.em_in_revenue,13) * round(HKD.reporting_exchange_rate,13)) AS em_in_hkd,
    SUM(round(Rev.gross_target_value_in_revenue,13) * round(HKD.reporting_exchange_rate,13)) AS gross_target_value_in_hkd,
    MAX(Rev.sd_row_creation) AS sd_row_creation,
    curdate() as etl_time,
    '{}' as sd_row_real_creation
FROM
    (select * from finance_bi.ods_finance_dw_tbl_fact_revenue_day_ef
    where fiscal_year_key >= (SELECT MAX(fiscal_year_key) FROM finance_bi.ods_finance_dw_tbl_fact_revenue_day_ef)-1) AS Rev
    INNER JOIN finance_bi.ods_finance_dw_tbl_dim_current_job_day_ei AS CJ
    ON Rev.current_job_key = CJ.current_job_key
    INNER JOIN finance_bi.ods_finance_dw_tbl_dim_current_client_day_ei AS CC
    ON Rev.client_key = CC.client_code
    INNER JOIN finance_bi.ods_finance_dw_tbl_dim_firm_structure_day_ef AS JobFS
    ON Rev.cy_job_ou_key = JobFS.ou_key
    LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_calendar_day_ef AS Cal
    ON Rev.date_key = Cal.date_key
    LEFT JOIN APC AS APC
    ON Rev.date_key = APC.sd_per_end_date
        AND Rev.current_job_key = APC.current_job_key
    LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_current_staff_day_ei AS JP
    ON APC.ch_asg_ptr_staff_code = JP.staff_code
    LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_exchange_rate_day_ef AS HKD
    ON Rev.exchange_rate_key = HKD.exchange_rate_key
        AND HKD.reporting_currency_code = 'HKD'
WHERE
    (
        JP.regional_staff_los_code like '%ADV%'
        OR (
            JP.regional_staff_los_code like '%OFS%'
            AND JP.regional_staff_sub_los_code like '%48%'
        )
    )
GROUP BY
    Cal.fiscal_year_name,
    Cal.month_end,
    JP.staff_name,
    JP.regional_staff_code,
    JP.regional_staff_ou_code,
    CC.uhc_name,
    CC.uhc_prid,
    Rev.client_key,
    CC.client_name,
    CJ.job_code,
    CJ.job_desc,
    CJ.job_office_code,
    CJ.product,
    JobFS.ou_code,
    JobFS.los
HAVING
    NOT(
        SUM(Rev.revenue) = 0
        AND SUM(Rev.em_in_revenue) = 0
        AND SUM(Rev.gross_target_value_in_revenue) = 0
        AND SUM(Rev.hours_in_revenue) = 0
    );
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
    r = conn.execute(text(SELECT_DEPENDENCE_DATE_SQL))
    dependence_date_flag = r.fetchone()[0]
    if dependence_date_flag == 1:
        print("依赖表数据日期为最新日期，开始执行")
        truncateTable(engine, table_name)
        min_time_result = conn.execute(text(SELECT_TARGET_MIN_DATE))
        min_time = min_time_result.fetchone()[0]
        conn.execute(text(INSERT_TARGET_SQL.format(min_time)))
        print("sql执行成功")
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
    targetTableName = "dwd_finance_revenue_data_day_ef"
    srcEngine = create_engine(
        f'mysql+pymysql://admin_user:{urlquote("6a!F@^ac*jBHtc7uUdxC")}@10.158.15.148:6030'
        f'/finance_bi')
    srcConn = srcEngine.connect()
    result_1 = srcConn.execute(text(
        f"select IF(date(sd_row_real_creation) = {CURR_DATE_STR}, 1, 0)as r from {targetTableName} limit 1"))
    # 获取第一条元素的第一个字段
    generate_target_table(targetTableName)
    # newestDateFlag = result_1.fetchone()[0]
    # srcConn.close()
    # if newestDateFlag == 0:
    #     print("数据不是最新日期，任务开始执行")
    #     generate_target_table(targetTableName)
    # else:
    #     print("数据日期为最新日期，无需执行")
