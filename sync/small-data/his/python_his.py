# !/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import urllib

TargetDatabase = "ces_new"
TargetTable = "dwd_security_details_day_ei"
TargetTableHis = "dwd_security_details_day_st"

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


def handle_his(engine):
    new_data_sql = f"select * from `{TargetDatabase}`.`{TargetTable}` where RowInsertDateTime"
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
    # etlPwcMDMData(tarEngine)
    print("update dwd vwFirmUserRole_his data is complete")


if __name__ == '__main__':
    main()
