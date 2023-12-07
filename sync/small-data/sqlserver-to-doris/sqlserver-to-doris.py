# !/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import urllib


def synSqlServerTable(srcEngine, tarEngine, table_name):
    srcConn = srcEngine.connect()
    tarConn = tarEngine.connect()
    pdSrc = pd.read_sql(text(f"select * from {table_name}"), srcConn)
    targetTableName = table_name.replace(".", "_")
    tarConn.execute(text(f"truncate table {targetTableName}"))
    print(targetTableName, ": table truncate is complete")
    pdSrc.to_sql(targetTableName, tarEngine, if_exists='append', index=False)
    print("data sync is complete")


def main():
    # pro sqlserver
    srcConnUrl = "mssql+pymssql://App_Airflow:%s@CNCSQLPWV5028:1800/PwCMDM"\
                 % (urllib.parse.quote_plus('P@ss1234567890'))
    # uat
    tarConnUrl = "mysql+pymysql://root@10.158.34.175:9030/PwCMDM"
    srcEngine = create_engine(srcConnUrl)
    tarEngine = create_engine(tarConnUrl)
    # Core.tblFirmStructure,Core.tblBusinessUnit,dbo.tblGroupCode,dbo.tblFirmServiceLineCode
    # tableNamesSet = "Core.tblFirmStructure,Core.tblBusinessUnit,dbo.tblGroupCode,dbo.tblFirmServiceLineCode".split(",")
    for tableName in tableNamesSet:
        print("sync ods vwFirmUserRole data start")
        synSqlServerTable(srcEngine, tarEngine, tableName)
        print("sync ods vwFirmUserRole data is complete")


if __name__ == '__main__':
    main()
