# !/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import urllib


def synSqlServerTable(srcEngine, tarEngine):
    srcConn = srcEngine.connect()
    tarConn = tarEngine.connect()
    pdSrc = pd.read_sql(text(
        "select  convert(VARCHAR(255),ID)as ID, trim(staffid) as staffid, staffname, rolecode, roledesc, officecode, groupcode, groupdesc, loscode, losdesc, countrycode, oustatus, daterminationdate from vwFirmUserRole"),
                        srcConn)
    tarConn.execute(text("truncate table dbo_vwFirmUserRole"))
    print("table truncate is complete")
    pdSrc.to_sql("dbo_vwFirmUserRole", tarEngine, if_exists='append', index=False)
    print("data update is complete")


def etlPwcMDMData(engine):
    sqlList = ["""insert into `PwcMDM_temp`.`dbo_vwFirmUserRole_his`(startDate, ID, StaffID, StaffName, RoleCode, RoleDesc, OfficeCode, GroupCode, GroupDesc, LoSCode, LoSDesc, CountryCode, OUStatus, daTerminationDate, endDate)
SELECT his.startDate,his.ID,his.StaffID,his.StaffName,his.RoleCode,his.RoleDesc,his.OfficeCode,his.GroupCode,his.GroupDesc,his.LoSCode,his.LoSDesc,his.CountryCode,his.OUStatus,his.daTerminationDate,CURDATE() as endDate
FROM (select * from `PwcMDM_temp`.`dbo_vwFirmUserRole_his` where endDate = '9999-12-31') AS his LEFT JOIN PwcMDM_temp.dbo_vwFirmUserRole AS org
ON his.ID = org.ID where org.ID IS NULL;""", """insert into `PwcMDM_temp`.`dbo_vwFirmUserRole_his`(startDate, ID, StaffID, StaffName, RoleCode, RoleDesc, OfficeCode, GroupCode, GroupDesc, LoSCode, LoSDesc, CountryCode, OUStatus, daTerminationDate, endDate)
select his.startDate, his.ID, his.StaffID, his.StaffName, his.RoleCode, his.RoleDesc, his.OfficeCode, his.GroupCode, his.GroupDesc, his.LoSCode, his.LoSDesc, his.CountryCode, his.OUStatus, his.daTerminationDate,CURDATE() as endDate
from(
     SELECT  ID,StaffID,StaffName,RoleCode,RoleDesc,OfficeCode,GroupCode,GroupDesc,LoSCode,LoSDesc,CountryCode,OUStatus,daTerminationDate
	FROM `PwcMDM_temp`.`dbo_vwFirmUserRole` EXCEPT SELECT ID,StaffID,StaffName,RoleCode,RoleDesc,OfficeCode,GroupCode,GroupDesc,LoSCode,LoSDesc,CountryCode,OUStatus,daTerminationDate
    FROM `PwcMDM_temp`.`dbo_vwFirmUserRole_his` WHERE endDate = '9999-12-31'
        ) as orgUpdate
join (select * from `PwcMDM_temp`.`dbo_vwFirmUserRole_his` where endDate = '9999-12-31') AS his
    on orgUpdate.ID = his.ID;""", """insert into `PwcMDM_temp`.`dbo_vwFirmUserRole_his`(startDate, ID, StaffID, StaffName, RoleCode, RoleDesc, OfficeCode, GroupCode, GroupDesc, LoSCode, LoSDesc, CountryCode, OUStatus, daTerminationDate, endDate)
select curdate() as startDate,orgNew.*,'9999-12-31' as endDate
from(
     SELECT  ID,StaffID,StaffName,RoleCode,RoleDesc,OfficeCode,GroupCode,GroupDesc,LoSCode,LoSDesc,CountryCode,OUStatus,daTerminationDate
	FROM `PwcMDM_temp`.`dbo_vwFirmUserRole` EXCEPT SELECT ID,StaffID,StaffName,RoleCode,RoleDesc,OfficeCode,GroupCode,GroupDesc,LoSCode,LoSDesc,CountryCode,OUStatus,daTerminationDate
    FROM `PwcMDM_temp`.`dbo_vwFirmUserRole_his` WHERE endDate = '9999-12-31'
        ) as orgNew
left join (select * from `PwcMDM_temp`.`dbo_vwFirmUserRole_his` where endDate = '9999-12-31') AS his
    on orgNew.ID = his.ID
where his.ID is null;"""]
    conn = engine.connect()
    for sql in sqlList:
        res = conn.execute(text(sql.replace("\n", " ")))
        print(sql, ",执行成功,受影响的行数: ", res.rowcount)


def main():
    # pro sqlserver
    # srcConnUrl = "mssql+pymssql://MiddlePlatform:%s@CNCSQLPWV5027:1800/PwcMDM_temp" % (
    #     urllib.parse.quote_plus('!QAZ@WSX#edc'))
    # uat
    tarConnUrl = "mysql+pymysql://root@10.158.34.175:9030/PwcMDM_temp"
    # srcEngine = create_engine(srcConnUrl)
    tarEngine = create_engine(tarConnUrl)
    # print("sync ods vwFirmUserRole data start")
    # synSqlServerTable(srcEngine, tarEngine)
    # print("sync ods vwFirmUserRole data is complete")
    print("update dwd vwFirmUserRole_his data start")
    etlPwcMDMData(tarEngine)
    print("update dwd vwFirmUserRole_his data is complete")


if __name__ == '__main__':
    main()
