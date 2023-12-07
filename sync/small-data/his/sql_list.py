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



#数据的补：
history_restore = [
#已经删除了的数据：
"""
insert into `WorkDayStage`.`HRWorkers_Certification_dwd_his`(startdate, Worker_ID, Certification_Reference_ID, Issued_Date, Certification_Name, Examination_Date, Examination_Score, Expiration_Date, enddate)
SELECT his.startDate,his.Worker_ID,his.Certification_Reference_ID,his.Issued_Date,his.Certification_Name,his.Examination_Date,his.Examination_Score,his.Expiration_Date,CURDATE() as endDate
FROM (
    select *
        from `WorkDayStage`.`HRWorkers_Certification_dwd_his` where endDate > CURDATE()
    ) AS his LEFT JOIN `WorkDayStage`.`HRWorkers_Certification_dwd` AS org
ON his.Worker_ID = org.Worker_ID AND his.Certification_Reference_ID = org.Certification_Reference_ID AND his.Issued_Date = org.Issued_Date where org.Worker_ID IS NULL;
""",
#已经更新了的数据：
"""
insert into `WorkDayStage`.`HRWorkers_Certification_dwd_his`(startdate, Worker_ID, Certification_Reference_ID, Issued_Date, Certification_Name, Examination_Date, Examination_Score, Expiration_Date, enddate)
select his.startdate, his.worker_id, his.certification_reference_id, his.issued_date, his.certification_name, his.examination_date, his.examination_score, his.expiration_date,CURDATE() as endDate
from(
     SELECT  Worker_ID, Certification_Reference_ID, Issued_Date, Certification_Name, Examination_Date, Examination_Score, Expiration_Date
	FROM `WorkDayStage`.`HRWorkers_Certification_dwd` EXCEPT SELECT Worker_ID, Certification_Reference_ID, Issued_Date, Certification_Name, Examination_Date, Examination_Score, Expiration_Date
    FROM `WorkDayStage`.`HRWorkers_Certification_dwd_his` WHERE endDate > CURDATE() AND startDate <= CURDATE()
        ) as orgUpdate
join (select * from `WorkDayStage`.`HRWorkers_Certification_dwd_his` where CURDATE() AND startDate <= CURDATE()) AS his
    on orgUpdate.Worker_ID = his.Worker_ID
           AND orgUpdate.Certification_Reference_ID = his.Certification_Reference_ID
           AND orgUpdate.Issued_Date = his.Issued_Date;
""",
#已经新增的数据
"""
insert into `WorkDayStage`.`HRJobProfiles_dwd_his`(startDate, Job_Code, Job_Title, `Inactive`, Management_Level_Reference_Name, Job_Level_Reference_Name, Job_Category_Reference_ID, Job_Level_Reference_ID, Management_Level_Reference_ID, endDate)
select curdate() as startDate,orgNew.*,'9999-12-31' as endDate
from(
     SELECT Job_Code, Job_Title, `Inactive`, Management_Level_Reference_Name, Job_Level_Reference_Name, Job_Category_Reference_ID, Job_Level_Reference_ID, Management_Level_Reference_ID
	FROM `WorkDayStage`.`HRJobProfiles_dwd` EXCEPT SELECT Job_Code, Job_Title, `Inactive`, Management_Level_Reference_Name, Job_Level_Reference_Name, Job_Category_Reference_ID, Job_Level_Reference_ID, Management_Level_Reference_ID
    FROM `WorkDayStage`.`HRJobProfiles_dwd_his` WHERE endDate > CURDATE() AND startDate <= CURDATE()
        ) as orgNew
left join (select * from `WorkDayStage`.`HRJobProfiles_dwd_his` where endDate > CURDATE() AND startDate <= CURDATE()) AS his
    on orgNew.Job_Code = his.Job_Code
where his.Job_Code is null;
"""]
