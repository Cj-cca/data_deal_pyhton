# !/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import urllib

certificationSqlList = ["""insert into `WorkDayStage`.`HRWorkers_Certification_dwd_his`(startdate, Worker_ID, Certification_Reference_ID, Issued_Date, Certification_Name, Examination_Date, Examination_Score, Expiration_Date, enddate)
SELECT his.startDate,his.Worker_ID,his.Certification_Reference_ID,his.Issued_Date,his.Certification_Name,his.Examination_Date,his.Examination_Score,his.Expiration_Date,CURDATE() as endDate
FROM (
    select *
    from `WorkDayStage`.`HRWorkers_Certification_dwd_his` where endDate = '9999-12-31'
    ) AS his LEFT JOIN `WorkDayStage`.`HRWorkers_Certification_dwd` AS org
ON his.Worker_ID = org.Worker_ID AND his.Certification_Reference_ID = org.Certification_Reference_ID AND his.Issued_Date = org.Issued_Date where org.Worker_ID IS NULL;
""", """insert into `WorkDayStage`.`HRWorkers_Certification_dwd_his`(startdate, Worker_ID, Certification_Reference_ID, Issued_Date, Certification_Name, Examination_Date, Examination_Score, Expiration_Date, enddate)
select his.startdate, his.worker_id, his.certification_reference_id, his.issued_date, his.certification_name, his.examination_date, his.examination_score, his.expiration_date,CURDATE() as endDate
from(
     SELECT  Worker_ID, Certification_Reference_ID, Issued_Date, Certification_Name, Examination_Date, Examination_Score, Expiration_Date
	FROM `WorkDayStage`.`HRWorkers_Certification_dwd` EXCEPT SELECT Worker_ID, Certification_Reference_ID, Issued_Date, Certification_Name, Examination_Date, Examination_Score, Expiration_Date
    FROM `WorkDayStage`.`HRWorkers_Certification_dwd_his` WHERE endDate = '9999-12-31'
        ) as orgUpdate
join (select * from `WorkDayStage`.`HRWorkers_Certification_dwd_his` where endDate = '9999-12-31') AS his
    on orgUpdate.Worker_ID = his.Worker_ID
           AND orgUpdate.Certification_Reference_ID = his.Certification_Reference_ID
           AND orgUpdate.Issued_Date = his.Issued_Date;
""", """insert into `WorkDayStage`.`HRWorkers_Certification_dwd_his`(startdate, Worker_ID, Certification_Reference_ID, Issued_Date, Certification_Name, Examination_Date, Examination_Score, Expiration_Date, enddate)
select curdate() as startDate,orgNew.*,'9999-12-31' as endDate
from(
     SELECT  Worker_ID, Certification_Reference_ID, Issued_Date, Certification_Name, Examination_Date, Examination_Score, Expiration_Date
	FROM `WorkDayStage`.`HRWorkers_Certification_dwd` EXCEPT SELECT Worker_ID, Certification_Reference_ID, Issued_Date, Certification_Name, Examination_Date, Examination_Score, Expiration_Date
    FROM `WorkDayStage`.`HRWorkers_Certification_dwd_his` WHERE endDate = '9999-12-31'
        ) as orgNew
left join (select * from `WorkDayStage`.`HRWorkers_Certification_dwd_his` where endDate = '9999-12-31') AS his
    on orgNew.Worker_ID = his.Worker_ID
           AND orgNew.Certification_Reference_ID = his.Certification_Reference_ID
           AND orgNew.Issued_Date = his.Issued_Date
where his.Worker_ID is null;"""]

educationSqlList = ["""insert into `WorkDayStage`.`HRWorkers_Education_dwd_his`(startDate, Worker_ID, Degree, First_Year_Attended, Is_Highest_Level_of_Education, Last_Year_Attended, School_ID, School_Name, Education_Country, Degree_Receiving_Date, Field_Of_Study, endDate)
SELECT his.startDate, his.Worker_ID, his.Degree, his.First_Year_Attended, his.Is_Highest_Level_of_Education, his.Last_Year_Attended, his.School_ID, his.School_Name, his.Education_Country, his.Degree_Receiving_Date, his.Field_Of_Study, CURDATE() as endDate
    FROM (select * from `WorkDayStage`.`HRWorkers_Education_dwd_his` where endDate = '9999-12-31') AS his
    LEFT JOIN `WorkDayStage`.`HRWorkers_Education_dwd` AS org
    ON his.Worker_ID = org.Worker_ID AND his.Degree = org.Degree
    where org.Worker_ID IS NULL;""", """insert into `WorkDayStage`.`HRWorkers_Education_dwd_his`(startDate, Worker_ID, Degree, First_Year_Attended, Is_Highest_Level_of_Education, Last_Year_Attended, School_ID, School_Name, Education_Country, Degree_Receiving_Date, Field_Of_Study, endDate)
select his.startDate, his.Worker_ID, his.Degree, his.First_Year_Attended, his.Is_Highest_Level_of_Education, his.Last_Year_Attended, his.School_ID, his.School_Name, his.Education_Country, his.Degree_Receiving_Date, his.Field_Of_Study,CURDATE() as endDate
from(
     SELECT  Worker_ID, Degree, First_Year_Attended, Is_Highest_Level_of_Education, Last_Year_Attended, School_ID, School_Name, Education_Country, Degree_Receiving_Date, Field_Of_Study
	FROM `WorkDayStage`.`HRWorkers_Education_dwd` EXCEPT SELECT Worker_ID, Degree, First_Year_Attended, Is_Highest_Level_of_Education, Last_Year_Attended, School_ID, School_Name, Education_Country, Degree_Receiving_Date, Field_Of_Study
    FROM `WorkDayStage`.`HRWorkers_Education_dwd_his` WHERE endDate = '9999-12-31'
        ) as orgUpdate
join (select * from `WorkDayStage`.`HRWorkers_Education_dwd_his` where endDate = '9999-12-31') AS his
    on orgUpdate.Worker_ID = his.Worker_ID AND orgUpdate.Degree = his.Degree;""", """insert into `WorkDayStage`.`HRWorkers_Education_dwd_his`(startDate, Worker_ID, Degree, First_Year_Attended, Is_Highest_Level_of_Education, Last_Year_Attended, School_ID, School_Name, Education_Country, Degree_Receiving_Date, Field_Of_Study, endDate)
select curdate() as startDate,orgNew.*,'9999-12-31' as endDate
from(
     SELECT  Worker_ID, Degree, First_Year_Attended, Is_Highest_Level_of_Education, Last_Year_Attended, School_ID, School_Name, Education_Country, Degree_Receiving_Date, Field_Of_Study
	FROM `WorkDayStage`.`HRWorkers_Education_dwd` EXCEPT SELECT Worker_ID, Degree, First_Year_Attended, Is_Highest_Level_of_Education, Last_Year_Attended, School_ID, School_Name, Education_Country, Degree_Receiving_Date, Field_Of_Study
    FROM `WorkDayStage`.`HRWorkers_Education_dwd_his` WHERE endDate = '9999-12-31'
        ) as orgNew
left join (select * from `WorkDayStage`.`HRWorkers_Education_dwd_his` where endDate = '9999-12-31') AS his
    on orgNew.Worker_ID = his.Worker_ID AND orgNew.Degree = his.Degree
where his.Worker_ID is null;"""]

jobProfilesSqlList = ["""insert into `WorkDayStage`.`HRJobProfiles_dwd_his`(startDate, Job_Code, Job_Title, Inactive, Management_Level_Reference_Name, Job_Level_Reference_Name, Job_Category_Reference_ID, Job_Level_Reference_ID, Management_Level_Reference_ID, endDate)
SELECT his.startDate, his.Job_Code, his.Job_Title, his.Inactive, his.Management_Level_Reference_Name, his.Job_Level_Reference_Name, his.Job_Category_Reference_ID, his.Job_Level_Reference_ID, his.Management_Level_Reference_ID, CURDATE() as endDate
    FROM (select * from `WorkDayStage`.`HRJobProfiles_dwd_his` where endDate = '9999-12-31') AS his
    LEFT JOIN `WorkDayStage`.`HRJobProfiles_dwd` AS org
    ON his.Job_Code = org.Job_Code
    where org.Job_Code IS NULL;""", """insert into `WorkDayStage`.`HRJobProfiles_dwd_his`(startDate, Job_Code, Job_Title, `Inactive`, Management_Level_Reference_Name, Job_Level_Reference_Name, Job_Category_Reference_ID, Job_Level_Reference_ID, Management_Level_Reference_ID, endDate)
select his.startDate, his.Job_Code, his.Job_Title, his.`Inactive`, his.Management_Level_Reference_Name, his.Job_Level_Reference_Name, his.Job_Category_Reference_ID, his.Job_Level_Reference_ID, his.Management_Level_Reference_ID,CURDATE() as endDate
from(
     SELECT  Job_Code, Job_Title, `Inactive`, Management_Level_Reference_Name, Job_Level_Reference_Name, Job_Category_Reference_ID, Job_Level_Reference_ID, Management_Level_Reference_ID
	FROM `WorkDayStage`.`HRJobProfiles_dwd` EXCEPT SELECT Job_Code, Job_Title, `Inactive`, Management_Level_Reference_Name, Job_Level_Reference_Name, Job_Category_Reference_ID, Job_Level_Reference_ID, Management_Level_Reference_ID
    FROM `WorkDayStage`.`HRJobProfiles_dwd_his` WHERE endDate = '9999-12-31'
        ) as orgUpdate
join (select * from `WorkDayStage`.`HRJobProfiles_dwd_his` where endDate = '9999-12-31') AS his
    on orgUpdate.Job_Code = his.Job_Code;""", """insert into `WorkDayStage`.`HRJobProfiles_dwd_his`(startDate, Job_Code, Job_Title, `Inactive`, Management_Level_Reference_Name, Job_Level_Reference_Name, Job_Category_Reference_ID, Job_Level_Reference_ID, Management_Level_Reference_ID, endDate)
select curdate() as startDate,orgNew.*,'9999-12-31' as endDate
from(
     SELECT Job_Code, Job_Title, `Inactive`, Management_Level_Reference_Name, Job_Level_Reference_Name, Job_Category_Reference_ID, Job_Level_Reference_ID, Management_Level_Reference_ID
	FROM `WorkDayStage`.`HRJobProfiles_dwd` EXCEPT SELECT Job_Code, Job_Title, `Inactive`, Management_Level_Reference_Name, Job_Level_Reference_Name, Job_Category_Reference_ID, Job_Level_Reference_ID, Management_Level_Reference_ID
    FROM `WorkDayStage`.`HRJobProfiles_dwd_his` WHERE endDate = '9999-12-31'
        ) as orgNew
left join (select * from `WorkDayStage`.`HRJobProfiles_dwd_his` where endDate = '9999-12-31') AS his
    on orgNew.Job_Code = his.Job_Code
where his.Job_Code is null;"""]



"""
insert into `ces_new`.`dwd_security_identifier_flat_day_st`(start_date, security_id, cus_ip, is_in, security_identifier_types, `values`, security_identifier_type_value_pair, is_etl_deleted, lineage_id, row_insert_date_time, row_update_date_time,  end_date)
select start_date, security_id, cus_ip, is_in, security_identifier_types, `values`, security_identifier_type_value_pair, is_etl_deleted, lineage_id, row_insert_date_time, row_update_date_time,  CURDATE() as end_date
from `ces_new`.`dwd_security_identifier_flat_day_st` where end_date = '9999-12-31'
                                                                and security_id in (select security_id from(
     SELECT  security_id, cus_ip, is_in, security_identifier_types, `values`, security_identifier_type_value_pair, is_etl_deleted, lineage_id, row_insert_date_time, row_update_date_time
	FROM `ces_new`.`dwd_security_identifier_flat_day_ei` where row_insert_date_time > ${start_dt}
     EXCEPT SELECT security_id, cus_ip, is_in, security_identifier_types, `values`, security_identifier_type_value_pair, is_etl_deleted, lineage_id, row_insert_date_time, row_update_date_time
    FROM `ces_new`.`dwd_security_identifier_flat_day_st` WHERE end_date = '9999-12-31'
        ) as org);
insert into `ces_new`.`dwd_security_identifier_flat_day_st`(start_date, security_id, cus_ip, is_in, security_identifier_types, `values`, security_identifier_type_value_pair, is_etl_deleted, lineage_id, row_insert_date_time, row_update_date_time, end_date)
select curdate() as start_date,org.*,'9999-12-31' as end_date
from(
     SELECT  security_id, cus_ip, is_in, security_identifier_types, `values`, security_identifier_type_value_pair, is_etl_deleted, lineage_id, row_insert_date_time, row_update_date_time
	FROM `ces_new`.`dwd_security_identifier_flat_day_ei` where row_insert_date_time > ${start_dt}
     EXCEPT SELECT security_id, cus_ip, is_in, security_identifier_types, `values`, security_identifier_type_value_pair, is_etl_deleted, lineage_id, row_insert_date_time, row_update_date_time
    FROM `ces_new`.`dwd_security_identifier_flat_day_st` WHERE end_date = '9999-12-31'
        ) as org;
"""


def etlHistoryTableData(engine, sqlList):
    conn = engine.connect()
    for sql in sqlList:
        res = conn.execute(text(sql.replace("\n", " ")))
        print(sql, ",执行成功,受影响的行数: ", res.rowcount)


def main():
    tarConnUrl = "mysql+pymysql://root@10.158.16.244:9030/WorkDayStage"
    tarEngine = create_engine(tarConnUrl)
    print("update dwd vwFirmUserRole_his data start")
    etlHistoryTableData(tarEngine, certificationSqlList)
    etlHistoryTableData(tarEngine, educationSqlList)
    etlHistoryTableData(tarEngine, jobProfilesSqlList)
    print("update dwd vwFirmUserRole_his data is complete")


if __name__ == '__main__':
    main()
