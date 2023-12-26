insert into assurance_portfolio_manager.dwd_assurance_job_revenue_day_ef
SELECT
    CJ.job_code,
    JFS.ou_code AS job_ou_code,
    JFS.ou AS job_ou,
    Cal.fiscal_year,
    Cal.month_end,
    WIP.exchange_rate_key,
    APC.ch_asg_ptr_staff_code AS job_partner_staff_code,
    JP.staff_name AS job_partner,
    APC.ch_asg_mgr_staff_code AS job_manager_staff_code,
    JM.staff_name AS job_manager,
    CC.uhc_name,
    CC.client_code,
    CC.client_name,
    CC.client_sector,
    CC.is_china_soe_uhc,
    SUM(WIP.revenue) AS revenue,
    MAX(WIP.sd_row_creation) AS sd_row_creation,
    now() AS warehouse_creation
FROM
    (select * from finance_bi.ods_finance_dw_tbl_fact_revenue_day_ef where date_key >= '2022-07-15') AS WIP
    INNER JOIN (select ou_code,ou,ou_key from finance_bi.ods_finance_dw_tbl_dim_firm_structure_day_ef where reporting_los = 'Assurance') AS JFS
    ON WIP.cy_job_ou_key = JFS.ou_key
    INNER JOIN finance_bi.ods_finance_dw_tbl_dim_calendar_day_ef AS Cal
    ON WIP.date_key = Cal.date_key
    INNER JOIN (select job_code,current_job_key from finance_bi.ods_finance_dw_tbl_dim_current_job_day_ei where job_territory_code = 'CN') AS CJ
    ON WIP.current_job_key = CJ.current_job_key
    INNER JOIN finance_bi.ods_finance_dw_tbl_dim_current_client_day_ei AS CC
    ON WIP.client_key = CC.client_code
    LEFT JOIN pwc_mdm.ods_ipower_tbl_assg_period_close_cn_day_ei AS APC
    ON WIP.date_key = APC.sd_per_end_date
        AND WIP.current_job_key = CONCAT('CN-', APC.int_assgnmt_id)
    LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_current_staff_day_ei AS JP
    ON APC.ch_asg_ptr_staff_code = JP.staff_code
    LEFT JOIN finance_bi.ods_finance_dw_tbl_dim_current_staff_day_ei AS JM
    ON APC.ch_asg_mgr_staff_code = JM.staff_code
WHERE
    NOT EXISTS(
        SELECT 1
        FROM finance_bi.ods_finance_dw_tbl_dim_job_revenue_exclusion_day_ef AS JRE
        WHERE WIP.job_revenue_exclusion_key = JRE.job_revenue_exclusion_key
    )
GROUP BY
    Cal.fiscal_year,
    Cal.month_end,
    WIP.exchange_rate_key,
    APC.ch_asg_ptr_staff_code,
    JP.staff_name,
    APC.ch_asg_mgr_staff_code,
    JM.staff_name,
    CC.uhc_name,
    CC.client_code,
    CC.client_name,
    CC.client_sector,
    CC.is_china_soe_uhc,
    CJ.job_code,
    JFS.ou_code,
    JFS.ou
HAVING
    SUM(WIP.revenue) <> 0;




insert into advisory_engagement_lifecycle.ods_advisory_talent_link_update
select staff_id,
       job_id,
       employee_id,
       start_date,
       country_code,
       worker_id,
       office_code,
       job_code,
       client_code,
       holiday_flag,
       work_hours,
       loading,
       end_date,
       term_flag,
       staff_name,
       job_title,
       create_by_date,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null
from advisory_engagement_lifecycle.ods_advisory_talent_link;

#获取res_id和job_id_desc
# insert into advisory_engagement_lifecycle.ods_advisory_talent_link_update_new
# select org.staff_id,
#        org.job_id,
#        org.employee_id,
#        org.start_date,
#        org.country_code,
#        org.worker_id,
#        org.office_code,
#        org.job_code,
#        org.client_code,
#        org.holiday_flag,
#        org.work_hours,
#        org.loading,
#        org.end_date,
#        org.term_flag,
#        org.staff_name,
#        org.job_title,
#        org.create_by_date,
#        org.res_id,
#        org.eng_partner_director,
#        org.eng_partner_director_id,
#        org.inet_email,
#        org.cost_centre,
#        org.cost_centre_code,
#        org.client_name,
#        org.job_id_desc,
#        concat(tmp.StartDate, ' - ', tmp.EndDate) AS data_range
# from advisory_engagement_lifecycle.ods_advisory_talent_link_update as org
#          left join (select JobID, WorkerID, RES_ID, JOB_ID_DESCR, StartDate, EndDate
#                     from talenlink.tblTalentLinkOrignal
#                     group by JobID, WorkerID, RES_ID, JOB_ID_DESCR, StartDate, EndDate) as tmp
#                    on org.job_id = tmp.JobID and org.worker_id = tmp.WorkerID and unix_timestamp(org.start_date) >= unix_timestamp(tmp.StartDate) and
#                       unix_timestamp(org.start_date) <= unix_timestamp(tmp.EndDate);


#获取res_id和job_id_desc
insert into advisory_engagement_lifecycle.ods_advisory_talent_link_update
select staff_id,
       job_id,
       employee_id,
       start_date,
       country_code,
       worker_id,
       office_code,
       job_code,
       client_code,
       holiday_flag,
       work_hours,
       loading,
       end_date,
       term_flag,
       staff_name,
       job_title,
       create_by_date,
       tmp.RES_ID       as res_id,
       eng_partner_director,
       eng_partner_director_id,
       inet_email,
       cost_centre,
       cost_centre_code,
       client_name,
       tmp.JOB_ID_DESCR as job_id_desc,
       date_range
from advisory_engagement_lifecycle.ods_advisory_talent_link_update as org
         left join (select JobID, WorkerID, RES_ID, JOB_ID_DESCR
                    from talenlink.tblTalentLinkOrignal
                    group by JobID, WorkerID, RES_ID, JOB_ID_DESCR) as tmp
                   on org.job_id = tmp.JobID and org.worker_id = tmp.WorkerID;


#获取cost_centre_code,cost_centre,email(都能关联上)
insert into advisory_engagement_lifecycle.ods_advisory_talent_link_update_new
select org.staff_id,
       org.job_id,
       org.employee_id,
       org.start_date,
       org.country_code,
       org.worker_id,
       org.office_code,
       org.job_code,
       org.client_code,
       org.holiday_flag,
       org.work_hours,
       org.loading,
       org.end_date,
       org.term_flag,
       org.staff_name,
       org.job_title,
       org.create_by_date,
       org.res_id,
       org.eng_partner_director,
       org.eng_partner_director_id,
       tar.inet_email,
       tar.cost_centre,
       tar.cost_centre_code,
       org.client_name,
       org.job_id_desc,
       org.date_range
from advisory_engagement_lifecycle.ods_advisory_talent_link_newest as org
         left join (
    select tbl_talenlink.staff_id,
           concat(SUBSTRING_INDEX(tbl_staff_bank.group_name, ' ', 1), tbl_staff_bank.group_code) as cost_centre_code,
           substring(tbl_staff_bank.group_name, 5)                                               as cost_centre,
           tbl_staff_bank.inet_email
    from (
             select staff_id
             from advisory_engagement_lifecycle.ods_advisory_talent_link_newest
             group by staff_id) as tbl_talenlink
             left join staff_bank.ods_hr_staffbank_day_ei as tbl_staff_bank
                       on tbl_talenlink.staff_id = tbl_staff_bank.staff_id) as tar
                   on org.staff_id = tar.staff_id;

#获取client_name
insert into advisory_engagement_lifecycle.ods_advisory_talent_link_update_new
select org.staff_id,
       org.job_id,
       org.employee_id,
       org.start_date,
       org.country_code,
       org.worker_id,
       org.office_code,
       org.job_code,
       org.client_code,
       org.holiday_flag,
       org.work_hours,
       org.loading,
       org.end_date,
       org.term_flag,
       org.staff_name,
       org.job_title,
       org.create_by_date,
       org.res_id,
       org.eng_partner_director,
       org.eng_partner_director_id,
       org.inet_email,
       org.cost_centre,
       org.cost_centre_code,
       tar.client_name,
       org.job_id_desc,
       org.date_range
from advisory_engagement_lifecycle.ods_advisory_talent_link_update_new as org
         left join (
    select ClientCode, tbl_client.nvc_client_name as client_name
    from (select ClientCode from talenlink.tblTalentLinkOrignal group by ClientCode) as tbl_talentlink
             left join pwc_mdm.ods_fin_tbl_client_day_ef as tbl_client
                       on tbl_talentlink.ClientCode = tbl_client.ch_client_code) as tar
                   on org.client_code = tar.ClientCode;


#获取partner_id and partner_name，匹配ch_job_partner_code能直接找到的的部分
insert into advisory_engagement_lifecycle.ods_advisory_talent_link_update_new
select org.staff_id,
       org.job_id,
       org.employee_id,
       org.start_date,
       org.country_code,
       org.worker_id,
       org.office_code,
       org.job_code,
       org.client_code,
       org.holiday_flag,
       org.work_hours,
       org.loading,
       org.end_date,
       org.term_flag,
       org.staff_name,
       org.job_title,
       org.create_by_date,
       org.res_id,
       tar.eng_partner_director,
       tar.eng_partner_director_id,
       org.inet_email,
       org.cost_centre,
       org.cost_centre_code,
       org.client_name,
       org.job_id_desc,
       org.date_range
from advisory_engagement_lifecycle.ods_advisory_talent_link_update_new as org
         left join(
    select a1.client_code,
           a1.job_code,
           a1.office_code,
           tbl_job.ch_job_partner_code as eng_partner_director_id,
           tbl_staff_bank.staff_name   as eng_partner_director
    from (
             select client_code, job_code, office_code
             from advisory_engagement_lifecycle.ods_advisory_talent_link_update_new
             group by client_code, job_code, office_code) as a1
             left join pwc_mdm.ods_fin_tbl_job_day_ef as tbl_job
                       on a1.client_code = tbl_job.ch_client_code and a1.job_code = tbl_job.ch_job_code
                           and a1.office_code = tbl_job.ch_job_office_code
             left join staff_bank.ods_hr_staffbank_day_ei as tbl_staff_bank
                       on tbl_job.ch_job_partner_code = tbl_staff_bank.staff_id
    where staff_name is not null) as tar
                  on org.client_code = tar.client_code and org.job_code = tar.job_code and
                     org.office_code = tar.office_code;


#获取partner_id and partner_name，匹配ch_job_partner_code需要处理才能找到的部分
insert into advisory_engagement_lifecycle.ods_advisory_talent_link_update_new
select org_out.staff_id,
       org_out.job_id,
       org_out.employee_id,
       org_out.start_date,
       org_out.country_code,
       org_out.worker_id,
       org_out.office_code,
       org_out.job_code,
       org_out.client_code,
       org_out.holiday_flag,
       org_out.work_hours,
       org_out.loading,
       org_out.end_date,
       org_out.term_flag,
       org_out.staff_name,
       org_out.job_title,
       org_out.create_by_date,
       org_out.res_id,
       tar_out.eng_partner_director,
       tar_out.ch_job_partner_code,
       org_out.inet_email,
       org_out.cost_centre,
       org_out.cost_centre_code,
       org_out.client_name,
       org_out.job_id_desc,
       org_out.date_range
from advisory_engagement_lifecycle.ods_advisory_talent_link_update_new as org_out
         left join(
    select org.*, tar.staff_name as eng_partner_director
    from (
             select a1.client_code, a1.job_code, a1.office_code, tbl_job.ch_job_partner_code
             from (
                      select client_code, job_code, office_code
                      from advisory_engagement_lifecycle.ods_advisory_talent_link_update_new
                      group by client_code, job_code, office_code) as a1
                      left join pwc_mdm.ods_fin_tbl_job_day_ef as tbl_job
                                on a1.client_code = tbl_job.ch_client_code and a1.job_code = tbl_job.ch_job_code
                                    and a1.office_code = tbl_job.ch_job_office_code
                      left join staff_bank.ods_hr_staffbank_day_ei as tbl_staff_bank
                                on tbl_job.ch_job_partner_code = tbl_staff_bank.staff_id
             where staff_name is null) as org
             left join(
        select org.ch_job_partner_code, tbl_staff_bank_new.staff_name
        from (
                 select tbl_job.ch_job_partner_code
                 from (
                          select client_code, job_code, office_code
                          from advisory_engagement_lifecycle.ods_advisory_talent_link_update_new
                          group by client_code, job_code, office_code) as a1
                          left join pwc_mdm.ods_fin_tbl_job_day_ef as tbl_job
                                    on a1.client_code = tbl_job.ch_client_code and a1.job_code = tbl_job.ch_job_code
                                        and a1.office_code = tbl_job.ch_job_office_code
                          left join staff_bank.ods_hr_staffbank_day_ei as tbl_staff_bank
                                    on tbl_job.ch_job_partner_code = tbl_staff_bank.staff_id
                 where staff_name is null
                 group by ch_job_partner_code) as org
                 left join staff_bank.ods_hr_staffbank_day_ei as tbl_staff_bank_new
                           on tbl_staff_bank_new.staff_id = right(org.ch_job_partner_code, 6)
                               or tbl_staff_bank_new.staff_id = concat('CN', right(org.ch_job_partner_code, 6))
                               or tbl_staff_bank_new.staff_id = cast(right(org.ch_job_partner_code, 6) as int)
        where staff_name is not null) as tar
                      on org.ch_job_partner_code = tar.ch_job_partner_code) as tar_out
                  on org_out.client_code = tar_out.client_code and org_out.job_code = tar_out.job_code and
                     org_out.office_code = tar_out.office_code;
