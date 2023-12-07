# !/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from ftplib import FTP
from datetime import datetime, timedelta
import pymysql
import pandas as pd
import sys


class MyFTP(FTP):
    encoding = "utf-8"  # 默认编码

    def getSubdir(self, *args):
        '''拷贝了 nlst() 和 dir() 代码修改，返回详细信息而不打印'''
        cmd = 'LIST'
        func = None
        if args[-1:] and type(args[-1]) != type(''):
            args, func = args[:-1], args[-1]
        for arg in args:
            cmd = cmd + (' ' + arg)
        files = []
        self.retrlines(cmd, files.append)
        return files

    def getdirs(self, dirname=None):
        """返回目录列表，包括文件简要信息"""
        if dirname != None:
            self.cwd(dirname)
        files = self.getSubdir()

        r_files = [file.split(" ")[-1] for file in files]
        # 去除. ..
        return [file for file in r_files if file != "." and file != ".."]

    def getfiles(self, dirname=None):
        """返回文件列表，简要信息"""
        if dirname != None:
            self.cwd(dirname)  # 设置FTP当前操作的路径
        return self.nlst()  # 获取目录下的文件
    # 这个感觉有点乱，后面再说,
    # def getalldirs(self, dirname=None):
    #     """返回文件列表，获取整个ftp所有文件夹和文件名称简要信息"""
    #     if dirname != None:
    #         self.cwd(dirname)  # 设置FTP当前操作的路径
    #     files = []
    #     dirs = set(self.getdirs()) - set(self.getfiles())
    #     if dirs != {}:
    #         for name in dirs:
    #             self.cwd("..")  # 返回上级
    #             files += self.getalldirs(name)
    #     return files


class DorisConnection:
    conn = None
    cursor = None

    @staticmethod
    def getCursor(**kwargs):
        # 链接服务端
        if "port" not in kwargs or kwargs['port'] is None:
            kwargs['port'] = 9030
        if "charset" not in kwargs or kwargs['charset'] is None:
            kwargs['charset'] = "utf8"
        DorisConnection.conn = pymysql.connect(
            host=kwargs['host'],  # MySQL服务端的IP地址
            port=kwargs['port'],  # MySQL默认PORT地址(端口号)
            user=kwargs['user'],  # 用户名
            password=kwargs['password'],  # 密码,也可以简写为passwd
            database=kwargs['database'],  # 库名称,也可以简写为db
            charset=kwargs['charset']  # 字符编码
        )
        # 产生获取命令的游标对象
        # cursor = conn_obj.cursor()  # 括号内不写参数,数据是元组套元组
        DorisConnection.cursor = DorisConnection.conn.cursor(cursor=pymysql.cursors.DictCursor)  # 括号内写参数,数据会处理成字典形式
        return DorisConnection.cursor

    @staticmethod
    def close():
        if DorisConnection.cursor is not None:
            DorisConnection.cursor.close()
            DorisConnection.cursor = None

        if DorisConnection.conn is not None:
            DorisConnection.conn.close()
            DorisConnection.conn = None


class ExcelToDoris:
    __cursor = None
    __excel_map = {"股票退市资料（IO自用）": "`security_temp`.`ods_delisted_stock`",
                   "上市股票（含新三板）": "`security_temp`.`ods_listed_stock`",
                   "公募债券": "`security_temp`.`ods_public_offering_bonds`",
                   "可转债": "`security_temp`.`ods_convertible_bonds`",
                   "公募基金": "`security_temp`.`ods_public_offering_fund`",
                   "理财产品": "`security_temp`.`ods_wealth_management_product`",
                   "香港强基金": "`security_temp`.`ods_hk_mpf`",
                   "澳门央积金": "`security_temp`.`ods_macao_central_fund`",
                   "未上市股权": "`security_temp`.`ods_unlisted_stock`",
                   "其他（资管、私募、信托、结构性产品、pension&insu）": "`security_temp`.`ods_others_security`",
                   "Overseas security": "`security_temp`.`ods_overseas_security`",
                   "基金清算资料(IO自用)": "`security_temp`.`ods_fund_clear_info`"}

    __dtype={
      'PRID (Scheme)':str, 'Security ID (Scheme)':str, 'Security ID (CF)': str,
        'CF security linked entity PRID': str,'Security ID': str,
        'PRID': str, 'Subscription Ticker Symbol': str
       }

    def __init__(self, **kwargs):
        self.__cursor = kwargs['cursor']

    def truncateTable(self):
        for key in self.__excel_map:
            table = self.__excel_map[key]
            sql = " TRUNCATE TABLE  {} ".format(table)
            self.__cursor.execute(sql)

    def loadData(self, filePath, upsertTime):

        workbook = pd.read_excel(filePath, sheet_name=None, dtype=self.__dtype)

        for key in self.__excel_map:

            if key not in workbook:
                continue

            table = self.__excel_map[key]
            cache = "insert into  {}( `".format(table)
            meta_data = {}

            self.__cursor.execute("show full columns from {}".format(table))
            res = self.__cursor.fetchall()

            worksheet = workbook[key]
            sheetColumnList = worksheet.columns.values.tolist()
            passColumns = []
            for mid in sheetColumnList:
                sheetColumnName = str(mid).strip().replace(" - ", "_").replace(" ", "_").replace("-", "_").\
                    replace("/", "_or_").replace("(", "").replace(")", "").replace(".", "_").lower()

                errorField = True
                for temp in res:
                    if sheetColumnName == temp['Field']:
                        errorField = False
                        break

                if errorField:
                    passColumns.append(mid)
                    print(" meta data error ", table, sheetColumnName, mid)
                else:
                    meta_data[mid] = {"Type": str(temp['Type']).lower(), "Field": temp["Field"]}
            meta_data['update_time'] = {"Type": 'datetime', "Field": "update_time"}
            meta_data['create_time'] = {"Type": 'datetime', "Field": "create_time"}

            for key in meta_data:
                cache += meta_data[key]["Field"] + "`,`"
            cache = cache[:-2] + ") values "

            sql = ""
            batchSize = 1000
            count = 0
            for row in worksheet.values.tolist():

                sql += "("
                for rowData, columnName in zip(row, sheetColumnList):

                    if columnName in passColumns:
                        pass
                    elif pd.isna(rowData) and "varchar" not in meta_data[columnName]["Type"]:
                        sql += "null,"
                    elif pd.isna(rowData) or str(rowData).strip() == '':
                        sql += "'',"
                    elif type(rowData) == float:
                        sql += str(rowData) + ","
                    elif type(rowData) == datetime:
                        sql += "'" + rowData.strftime('%Y-%m-%d %H:%M:%S') + "',"
                    else:
                        sql += "'" + str(rowData).strip().replace("'", "\\'") + "',"

                sql += "'" + upsertTime + "',"
                sql += "'" + upsertTime + "'),"
                count += 1

                if count % batchSize == 0:
                    sql = cache + sql[:-1]
                    try:
                        res = self.__cursor.execute(sql)
                    except:
                        print("mid phase ERROR:\t", sql.replace("),(", "),\n("))
                    if res < batchSize:
                        print(table, count - batchSize, count, res, sep="\t\t")
                        print("mid phase SQL ERROR:\t", sql.replace("),(", "),\n("))
                    sql = ""

            if count % batchSize > 0:
                sql = cache + sql[: -1]
                try:
                    res = self.__cursor.execute(sql)
                except:
                    print("final phase ERROR:\t", sql.replace("),(", "),\n("))
                if res < count % batchSize:
                    print(table, count % batchSize, count, res, sep="\t\t")
                    print("final phase SQL ERROR:\t", sql.replace("),(", "),\n("))


def download(host, port, user, pwd, interval=5600):
    fileList = []
    ftp = MyFTP()  # 实例化
    ftp.connect(host, port)  # 连接
    ftp.login(user, pwd)  # 登录
    lst = ftp.getdirs()  # 返回目录下文件夹和文件列表
    today = datetime.now()
    filterTime = (today - timedelta(seconds=interval))
    for fileName in lst:
        fileTime = fileName.split(".")[0]
        res = datetime.strptime(fileTime, '%Y%m%d%H%M%S')
        if res > filterTime:
            bufsize = 2048  # 设置的缓冲区大小
            filename = "."  # 需要下载的文件
            if bool(1 - os.path.exists(filename)):
                os.mkdir(filename)
            filename = filename + "/" + fileName
            fd = open(filename, "wb")  # 以写模式在本地打开文件
            ftp.retrbinary("RETR {}".format(fileName), fd.write, bufsize)  # 接收服务器上文件并写入本地文件
            fileList.append(filename)
    ftp.quit()
    return fileList


def etlSecurityData(cursor):
    sqlList = [""" insert into  `security`.`ods_delisted_stock`( delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_,update_time,create_time) 
 SELECT  delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_  FROM  `security_temp`.`ods_delisted_stock`
 EXCEPT 
 SELECT  delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_  FROM  `security`.`ods_delisted_stock` 
) AS tb0 
LEFT JOIN `security`.`ods_delisted_stock` AS tb1 
ON tb0.`delisted_ticker_symbol` =tb1.`delisted_ticker_symbol` AND  tb0.`delisted_stock_name`=tb1.`delisted_stock_name`
  JOIN ( SELECT delisted_ticker_symbol,delisted_stock_name,update_time,create_time FROM `security_temp`.`ods_delisted_stock` ) AS tb2 
ON  tb0.`delisted_ticker_symbol` =tb2.`delisted_ticker_symbol` AND  tb0.`delisted_stock_name`=tb2.`delisted_stock_name`
) AS temp;""", """insert into  `security`.`ods_listed_stock`( neeq_listed_date,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time) 
 SELECT  neeq_listed_date,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  neeq_listed_date,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security_temp`.`ods_listed_stock`
 EXCEPT 
 SELECT  neeq_listed_date,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security`.`ods_listed_stock` 
) AS tb0 
LEFT JOIN `security`.`ods_listed_stock` AS tb1 
ON tb0.`ticker_symbol` =tb1.`ticker_symbol` AND  tb0.`security_type_en`=tb1.`security_type_en`
  JOIN ( SELECT `ticker_symbol`,`security_type_en`,update_time,create_time FROM `security_temp`.`ods_listed_stock` ) AS tb2 
ON  tb0.`ticker_symbol` =tb2.`ticker_symbol` AND  tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """, """ insert into  `security`.`ods_public_offering_bonds`( security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time) 
 SELECT  security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security_temp`.`ods_public_offering_bonds`
 EXCEPT 
 SELECT  security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security`.`ods_public_offering_bonds` 
) AS tb0 
LEFT JOIN `security`.`ods_public_offering_bonds` AS tb1 
ON tb0.`security_identifier` =tb1.`security_identifier` AND  tb0.`isin_code`=tb1.`isin_code`
  JOIN ( SELECT `security_identifier`, `isin_code`,update_time,create_time FROM `security_temp`.`ods_public_offering_bonds` ) AS tb2 
ON  tb0.`security_identifier` =tb2.`security_identifier` AND  tb0.`isin_code`=tb2.`isin_code`
) AS temp;""", """ insert into  `security`.`ods_convertible_bonds`( security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time) 
 SELECT  security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security_temp`.`ods_convertible_bonds`
 EXCEPT 
 SELECT  security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security`.`ods_convertible_bonds` 
) AS tb0 
LEFT JOIN `security`.`ods_convertible_bonds` AS tb1 
ON tb0.`security_identifier` =tb1.`security_identifier` AND  tb0.`ticker_symbol`=tb1.`ticker_symbol` AND tb0.`security_type_en`=tb1.`security_type_en`
  JOIN ( SELECT `security_identifier`,`ticker_symbol`,`security_type_en`,update_time,create_time FROM `security_temp`.`ods_convertible_bonds` ) AS tb2 
ON  tb0.`security_identifier` =tb2.`security_identifier` AND  tb0.`ticker_symbol`=tb2.`ticker_symbol` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp;""", """ insert into  `security`.`ods_public_offering_fund`( security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time) 
 SELECT  security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security_temp`.`ods_public_offering_fund`
 EXCEPT 
 SELECT  security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security`.`ods_public_offering_fund` 
) AS tb0 
LEFT JOIN `security`.`ods_public_offering_fund` AS tb1 
ON tb0.`security_identifier` =tb1.`security_identifier` AND tb0.`security_type_en`=tb1.`security_type_en`
  JOIN ( SELECT `security_identifier`,`security_type_en`,update_time,create_time FROM `security_temp`.`ods_public_offering_fund` ) AS tb2 
ON tb0.`security_identifier` =tb2.`security_identifier` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """, """insert into  `security`.`ods_wealth_management_product`( security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time) 
 SELECT  security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security_temp`.`ods_wealth_management_product`
 EXCEPT 
 SELECT  security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security`.`ods_wealth_management_product` 
) AS tb0 
LEFT JOIN `security`.`ods_wealth_management_product` AS tb1 
ON tb0.`security_id` =tb1.`security_id` AND tb0.`prid`=tb1.`prid` AND tb0.`security_type_en`=tb1.`security_type_en`
  JOIN ( SELECT `security_id`,`prid`,`security_type_en`,update_time,create_time FROM `security_temp`.`ods_wealth_management_product` ) AS tb2 
ON  tb0.`security_id` =tb2.`security_id` AND tb0.`prid`=tb2.`prid` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """,
               """insert into  `security`.`ods_hk_mpf`( security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time) 
 SELECT  security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security_temp`.`ods_hk_mpf`
 EXCEPT 
 SELECT  security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security`.`ods_hk_mpf` 
) AS tb0 
LEFT JOIN `security`.`ods_hk_mpf` AS tb1 
ON tb0.`security_id_scheme` =tb1.`security_id_scheme` AND tb0.`prid_scheme`=tb1.`prid_scheme` AND tb0.`security_id_cf`=tb1.`security_id_cf` AND tb0.`cf_security_linked_entity_prid`=tb1.`cf_security_linked_entity_prid`
  JOIN ( SELECT `security_id_scheme`, `prid_scheme`,`security_id_cf`,`cf_security_linked_entity_prid`,update_time,create_time FROM `security_temp`.`ods_hk_mpf` ) AS tb2 
ON  tb0.`security_id_scheme` =tb2.`security_id_scheme` AND tb0.`prid_scheme`=tb2.`prid_scheme` AND tb0.`security_id_cf`=tb2.`security_id_cf` AND tb0.`cf_security_linked_entity_prid`=tb2.`cf_security_linked_entity_prid`
) AS temp; """,
               """ insert into  `security`.`ods_macao_central_fund`( prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time) 
 SELECT  prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security_temp`.`ods_macao_central_fund`
 EXCEPT 
 SELECT  prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security`.`ods_macao_central_fund` 
) AS tb0 
LEFT JOIN `security`.`ods_macao_central_fund` AS tb1 
ON tb0.`prid` =tb1.`prid` AND tb0.`security_id`=tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
  JOIN ( SELECT `prid`,`security_id`,`security_type_en`,update_time,create_time FROM `security_temp`.`ods_macao_central_fund` ) AS tb2 
ON  tb0.`prid` =tb2.`prid` AND tb0.`security_id`=tb2.`security_id` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp;""", """insert into  `security`.`ods_unlisted_stock`( security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time) 
 SELECT  security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security_temp`.`ods_unlisted_stock`
 EXCEPT 
 SELECT  security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security`.`ods_unlisted_stock` 
) AS tb0 
LEFT JOIN `security`.`ods_unlisted_stock` AS tb1 
ON  tb0.`security_id` =tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
  JOIN ( SELECT `security_id`,`security_type_en`,update_time,create_time FROM `security_temp`.`ods_unlisted_stock` ) AS tb2 
ON  tb0.`security_id` =tb2.`security_id` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """, """ insert into  `security`.`ods_others_security`( security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time) 
 SELECT  security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security_temp`.`ods_others_security`
 EXCEPT 
 SELECT  security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security`.`ods_others_security` 
) AS tb0 
LEFT JOIN `security`.`ods_others_security` AS tb1 
ON tb0.`security_id` =tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
  JOIN ( SELECT `security_id`,`security_type_en`,update_time,create_time FROM `security_temp`.`ods_others_security` ) AS tb2 
ON  tb0.`security_id` =tb2.`security_id` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp;  """, """insert into  `security`.`ods_overseas_security`( security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time) 
 SELECT  security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security_temp`.`ods_overseas_security`
 EXCEPT 
 SELECT  security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id  FROM  `security`.`ods_overseas_security` 
) AS tb0 
LEFT JOIN `security`.`ods_overseas_security` AS tb1 
ON tb0.`security_id` =tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
  JOIN ( SELECT `security_id`,`security_type_en`,update_time,create_time FROM `security_temp`.`ods_overseas_security` ) AS tb2 
ON  tb0.`security_id` =tb2.`security_id` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """, """insert into  `security`.`ods_fund_clear_info`( security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_,update_time,create_time) 
 SELECT  security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_,update_time,create_time 
FROM ( 
 SELECT tb0.*,IF(tb1.create_time IS NULL,tb2.create_time,tb1.create_time) AS create_time,tb2.update_time 
 FROM (
 SELECT  security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_  FROM  `security_temp`.`ods_fund_clear_info`
 EXCEPT 
 SELECT  security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_  FROM  `security`.`ods_fund_clear_info` 
) AS tb0 
LEFT JOIN `security`.`ods_fund_clear_info` AS tb1 
ON tb0.`security_identifier` =tb1.`security_identifier` AND tb0.`fund_short_name_cn`=tb1.`fund_short_name_cn`
JOIN ( SELECT `security_identifier`, `fund_short_name_cn`,update_time,create_time FROM `security_temp`.`ods_fund_clear_info` ) AS tb2 
ON  tb0.`security_identifier` =tb2.`security_identifier` AND tb0.`fund_short_name_cn`=tb2.`fund_short_name_cn`
) AS temp; """]

    for sql in sqlList:
        cursor.execute(sql.replace("\n", " "))


def updateSecurityStatus(cursor):
    sqlList = [""" INSERT INTO  `security`.`ods_public_offering_fund`( security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time) 
 SELECT  security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
SELECT IF(tb2.security_identifier IS NULL,tb1.sec   urity_status, 'InActive') AS security_status,
IF(tb2.security_identifier IS NULL AND UNIX_TIMESTAMP(tb1.update_time)<UNIX_TIMESTAMP(tb2.update_time),tb2.update_time,tb1.update_time  ) AS update_time ,
tb1.security_identifier,security_type_en,security_type_cn,security_id,prid,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,create_time 
FROM `security`.`ods_public_offering_fund` AS tb1
LEFT JOIN (SELECT security_identifier,update_time  FROM `security`.`ods_fund_clear_info`) AS tb2
ON trim(tb1.security_identifier)=trim(tb2.security_identifier) 
) temp """, """ INSERT INTO  `security`.`ods_listed_stock`( 
ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,neeq_listed_date,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time ) 
SELECT  
ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,neeq_listed_date,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,update_time,create_time 
FROM ( 
SELECT IF(tb2.delisted_ticker_symbol IS NULL,tb1.security_status, 'InActive') AS security_status,
IF(tb2.delisted_ticker_symbol IS NULL AND UNIX_TIMESTAMP(tb1.update_time)<UNIX_TIMESTAMP(tb2.update_time),tb2.update_time,tb1.update_time  ) AS update_time ,
tb1.delisted_ticker_symbol,
ticker_symbol,security_type_en,security_id,prid,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,neeq_listed_date,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,create_time 
FROM `security`.`ods_listed_stock` AS tb1
LEFT JOIN (SELECT delisted_ticker_symbol,update_time  FROM `security`.`ods_delisted_stock`) AS tb2
ON trim(tb1.ticker_symbol)=trim(tb2.delisted_ticker_symbol) 
) temp; """]

    for sql in sqlList:
        cursor.execute(sql.replace("\n", " "))


def etlSecurityHisData(cursor):
    sqlMap = {"`security_his`.`dwd_delisted_stock`": {"update": """INSERT INTO `security_his`.`dwd_delisted_stock`(   end_dt,delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_,start_dt )
SELECT   update_time AS end_dt,delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_,start_dt 
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_
		FROM `security`.`ods_delisted_stock`
		EXCEPT
		SELECT   delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_
		FROM `security_his`.`dwd_delisted_stock`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_delisted_stock` AS tb1 
	ON tb0.`delisted_ticker_symbol` =tb1.`delisted_ticker_symbol` AND  tb0.`delisted_stock_name`=tb1.`delisted_stock_name`
	JOIN ( SELECT * FROM `security_his`.`dwd_delisted_stock` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON tb0.`delisted_ticker_symbol` =tb2.`delisted_ticker_symbol` AND  tb0.`delisted_stock_name`=tb2.`delisted_stock_name`
) AS temp; """, "insert": """ INSERT INTO `security_his`.`dwd_delisted_stock`(   end_dt,delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_,start_dt )
SELECT   end_dt,delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_,start_dt 
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_
		FROM `security`.`ods_delisted_stock`
		EXCEPT
		SELECT   delisted_ticker_symbol,delisted_stock_name,delisted_stock_name_en,delisted_date,delisted_share_price,delisted_bvps,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,no_
		FROM `security_his`.`dwd_delisted_stock`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_delisted_stock` AS tb1 
	ON tb0.`delisted_ticker_symbol` =tb1.`delisted_ticker_symbol` AND tb0.`delisted_stock_name`=tb1.`delisted_stock_name`
) AS temp; """},
              "`security_his`.`dwd_listed_stock`": {"update": """INSERT INTO `security_his`.`dwd_listed_stock`(   neeq_listed_date,end_dt,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   neeq_listed_date,update_time AS end_dt,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   neeq_listed_date,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_listed_stock`
		EXCEPT
		SELECT   neeq_listed_date,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_listed_stock`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_listed_stock` AS tb1 
	ON tb0.`ticker_symbol` =tb1.`ticker_symbol` AND  tb0.`security_type_en`=tb1.`security_type_en`
	JOIN ( SELECT * FROM `security_his`.`dwd_listed_stock` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON tb0.`ticker_symbol` =tb2.`ticker_symbol` AND  tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """, "insert": """INSERT INTO `security_his`.`dwd_listed_stock`(   neeq_listed_date,end_dt,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   neeq_listed_date,end_dt,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   neeq_listed_date,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_listed_stock`
		EXCEPT
		SELECT   neeq_listed_date,ticker_symbol,security_type_en,security_id,prid,security_status,security_type_cn,issuer_name_cn,issuer_name_en,stock_short_name_cn,stock_short_name_en,listed_date,listing_location_cn,listing_location_en,auditor,subscription_ticker_symbol,delisted_ticker_symbol,delisted_stock_name,delisted_date,delisted_reason_wind,ticker_symbol_after_restructuring,stock_short_name_after_restructuring,stock_short_name_after_restructuring_en,listed_date_after_restructuring,date_delisted_to_neeq,neeq_ticker_symbol,neeq_stock_short_name,neeq_stock_short_name_en,delisted_stock_type,stock_type_after_restructuring,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_listed_stock`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_listed_stock` AS tb1 
	ON tb0.`ticker_symbol` =tb1.`ticker_symbol` AND  tb0.`security_type_en`=tb1.`security_type_en`
) AS temp; 
 """},
              "`security_his`.`dwd_public_offering_bonds`": {"update": """ INSERT INTO `security_his`.`dwd_public_offering_bonds`(   end_dt,security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   update_time AS end_dt,security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_public_offering_bonds`
		EXCEPT
		SELECT   security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_public_offering_bonds`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_public_offering_bonds` AS tb1 
	ON tb0.`security_identifier` =tb1.`security_identifier` AND  tb0.`isin_code`=tb1.`isin_code`
	JOIN ( SELECT * FROM `security_his`.`dwd_public_offering_bonds` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON tb0.`security_identifier` =tb2.`security_identifier` AND  tb0.`isin_code`=tb2.`isin_code`
) AS temp; """, "insert": """INSERT INTO `security_his`.`dwd_public_offering_bonds`(   end_dt,security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   end_dt,security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_public_offering_bonds`
		EXCEPT
		SELECT   security_identifier,isin_code,security_id,prid,security_type_en,security_type_cn,security_status,security_short_name_cn,security_short_name_en,date_of_value,maturity_date,issuer_name_cn,issuer_name_en,auditor,listed_area,listed_date,listing_location_cn,listing_location_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_public_offering_bonds`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_public_offering_bonds` AS tb1 
	ON tb0.`security_identifier` =tb1.`security_identifier` AND  tb0.`isin_code`=tb1.`isin_code`
) AS temp;  """},
              "`security_his`.`dwd_convertible_bonds`": {"update": """INSERT INTO `security_his`.`dwd_convertible_bonds`(   end_dt,security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   update_time AS end_dt,security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_convertible_bonds`
		EXCEPT
		SELECT   security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_convertible_bonds`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_convertible_bonds` AS tb1 
	ON tb0.`security_identifier` =tb1.`security_identifier` AND  tb0.`ticker_symbol`=tb1.`ticker_symbol` AND tb0.`security_type_en`=tb1.`security_type_en`
	JOIN ( SELECT * FROM `security_his`.`dwd_convertible_bonds` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON tb0.`security_identifier` =tb2.`security_identifier` AND  tb0.`ticker_symbol`=tb2.`ticker_symbol` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """, "insert": """ INSERT INTO `security_his`.`dwd_convertible_bonds`(   end_dt,security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   end_dt,security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_convertible_bonds`
		EXCEPT
		SELECT   security_identifier,ticker_symbol,security_type_en,security_type_cn,security_id,prid,isin_code,security_status,security_short_name_cn,security_short_name_en,stock_short_name_cn,stock_short_name_en,date_of_value,maturity_date,auditor,listed_date,listing_location_cn,listing_location_en,issuer_name_cn,issuer_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_convertible_bonds`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_convertible_bonds` AS tb1 
	ON tb0.`security_identifier` =tb1.`security_identifier` AND  tb0.`ticker_symbol`=tb1.`ticker_symbol` AND tb0.`security_type_en`=tb1.`security_type_en`
) AS temp; """},
              "`security_his`.`dwd_public_offering_fund`": {"update": """INSERT INTO `security_his`.`dwd_public_offering_fund`(   end_dt,security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   update_time AS end_dt,security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_public_offering_fund`
		EXCEPT
		SELECT   security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_public_offering_fund`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_public_offering_fund` AS tb1 
	ON tb0.`security_identifier` =tb1.`security_identifier` AND tb0.`security_type_en`=tb1.`security_type_en`
	JOIN ( SELECT * FROM `security_his`.`dwd_public_offering_fund` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON tb0.`security_identifier` =tb2.`security_identifier` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """, "insert": """INSERT INTO `security_his`.`dwd_public_offering_fund`(   end_dt,security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   end_dt,security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_public_offering_fund`
		EXCEPT
		SELECT   security_identifier,security_type_en,security_type_cn,security_id,prid,security_status,isin_code,fund_short_name_cn,fund_short_name_en,fund_name_cn,fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,auditor,listing_location_cn,listing_location_en,subscription_security_identifier,fund_establish_date,fund_issuance_date,listed_date,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason_wind,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_public_offering_fund`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_public_offering_fund` AS tb1 
	ON tb0.`security_identifier` =tb1.`security_identifier` AND tb0.`security_type_en`=tb1.`security_type_en`
) AS temp;  """},
              "`security_his`.`dwd_wealth_management_product`": {"update": """INSERT INTO `security_his`.`dwd_wealth_management_product`(   end_dt,security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   update_time AS end_dt,security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_wealth_management_product`
		EXCEPT
		SELECT   security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_wealth_management_product`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_wealth_management_product` AS tb1 
	ON tb0.`security_id` =tb1.`security_id` AND tb0.`prid`=tb1.`prid` AND tb0.`security_type_en`=tb1.`security_type_en`
	JOIN ( SELECT * FROM `security_his`.`dwd_wealth_management_product` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON tb0.`security_id` =tb2.`security_id` AND tb0.`prid`=tb2.`prid` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """, "insert": """ INSERT INTO `security_his`.`dwd_wealth_management_product`(   end_dt,security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   end_dt,security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_wealth_management_product`
		EXCEPT
		SELECT   security_id,prid,security_type_en,security_type_cn,security_status,wealth_management_product_name_cn,wealth_management_product_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_wealth_management_product`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_wealth_management_product` AS tb1 
	ON tb0.`security_id` =tb1.`security_id` AND tb0.`prid`=tb1.`prid` AND tb0.`security_type_en`=tb1.`security_type_en`
) AS temp; """},
              "`security_his`.`dwd_hk_mpf`": {"update": """INSERT INTO `security_his`.`dwd_hk_mpf`(   end_dt,security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   update_time AS end_dt,security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_hk_mpf`
		EXCEPT
		SELECT   security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_hk_mpf`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_hk_mpf` AS tb1 
	ON tb0.`security_id_scheme` =tb1.`security_id_scheme` AND tb0.`prid_scheme`=tb1.`prid_scheme` AND tb0.`security_id_cf`=tb1.`security_id_cf` AND tb0.`cf_security_linked_entity_prid`=tb1.`cf_security_linked_entity_prid`
	JOIN ( SELECT * FROM `security_his`.`dwd_hk_mpf` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON  tb0.`security_id_scheme` =tb2.`security_id_scheme` AND tb0.`prid_scheme`=tb2.`prid_scheme` AND tb0.`security_id_cf`=tb2.`security_id_cf` AND tb0.`cf_security_linked_entity_prid`=tb2.`cf_security_linked_entity_prid`
) AS temp; """, "insert": """INSERT INTO `security_his`.`dwd_hk_mpf`(   end_dt,security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   end_dt,security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_hk_mpf`
		EXCEPT
		SELECT   security_id_scheme,prid_scheme,security_id_cf,cf_security_linked_entity_prid,security_status,security_type_en,security_type_cn,hongkong_mpf_scheme_name_cn,hongkong_mpf_scheme_name_en,hongkong_mpf_cf_name_cn,hongkong_mpf_cf_name_en,cf_fund_manager_or_advisor_name_cn,cf_fund_manager_or_advisor_name_en,underlying_fund_name_cn,underlying_fund_name_en,underlying_fund_manager_or_advisor_name_cn,underlying_fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_hk_mpf`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_hk_mpf` AS tb1 
	ON tb0.`security_id_scheme` =tb1.`security_id_scheme` AND tb0.`prid_scheme`=tb1.`prid_scheme` AND tb0.`security_id_cf`=tb1.`security_id_cf` AND tb0.`cf_security_linked_entity_prid`=tb1.`cf_security_linked_entity_prid`
) AS temp;  """},
              "`security_his`.`dwd_macao_central_fund`": {"update": """INSERT INTO `security_his`.`dwd_macao_central_fund`(   end_dt,prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   update_time AS end_dt,prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_macao_central_fund`
		EXCEPT
		SELECT   prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_macao_central_fund`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_macao_central_fund` AS tb1 
	ON tb0.`prid` =tb1.`prid` AND tb0.`security_id`=tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
	JOIN ( SELECT * FROM `security_his`.`dwd_macao_central_fund` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON tb0.`prid` =tb2.`prid` AND tb0.`security_id`=tb2.`security_id` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """, "insert": """ INSERT INTO `security_his`.`dwd_macao_central_fund`(   end_dt,prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   end_dt,prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_macao_central_fund`
		EXCEPT
		SELECT   prid,security_id,security_type_en,security_type_cn,security_status,macao_non_mandatory_central_provident_fund_name_cn,macao_non_mandatory_central_provident_fund_name_en,fund_manager_or_advisor_name_cn,fund_manager_or_advisor_name_en,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_macao_central_fund`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_macao_central_fund` AS tb1 
	ON tb0.`prid` =tb1.`prid` AND tb0.`security_id`=tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
) AS temp; """},
              "`security_his`.`dwd_unlisted_stock`": {"update": """INSERT INTO `security_his`.`dwd_unlisted_stock`(   end_dt,security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   update_time AS end_dt,security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_unlisted_stock`
		EXCEPT
		SELECT   security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_unlisted_stock`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_unlisted_stock` AS tb1 
	ON tb0.`security_id` =tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
	JOIN ( SELECT * FROM `security_his`.`dwd_unlisted_stock` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON tb0.`security_id` =tb2.`security_id` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """, "insert": """ INSERT INTO `security_his`.`dwd_unlisted_stock`(   end_dt,security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   end_dt,security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_unlisted_stock`
		EXCEPT
		SELECT   security_id,security_type_en,security_type_cn,security_status,prid,entity_name_cn,entity_name_en,nat_id_china_unified_social_credit_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_unlisted_stock`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_unlisted_stock` AS tb1 
	ON tb0.`security_id` =tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
) AS temp;  """},
              "`security_his`.`dwd_others_security`": {"update": """INSERT INTO `security_his`.`dwd_others_security`(   end_dt,security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   update_time AS end_dt,security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_others_security`
		EXCEPT
		SELECT   security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_others_security`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_others_security` AS tb1 
	ON tb0.`security_id` =tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
	JOIN ( SELECT * FROM `security_his`.`dwd_others_security` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON tb0.`security_id` =tb2.`security_id` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp; """, "insert": """ INSERT INTO `security_his`.`dwd_others_security`(   end_dt,security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   end_dt,security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_others_security`
		EXCEPT
		SELECT   security_id,security_type_en,security_type_cn,prid,security_status,product_name_cn,product_name_en,product_advisor_or_manager_cn,product_advisor_or_manager_en,product_code,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_others_security`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_others_security` AS tb1 
	ON tb0.`security_id` =tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
) AS temp; """},
              "`security_his`.`dwd_overseas_security`": {"update": """ INSERT INTO `security_his`.`dwd_overseas_security`(   end_dt,security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   update_time AS end_dt,security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_overseas_security`
		EXCEPT
		SELECT   security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_overseas_security`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_overseas_security` AS tb1 
	ON tb0.`security_id` =tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
	JOIN ( SELECT * FROM `security_his`.`dwd_overseas_security` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON tb0.`security_id` =tb2.`security_id` AND tb0.`security_type_en`=tb2.`security_type_en`
) AS temp;""", "insert": """INSERT INTO `security_his`.`dwd_overseas_security`(   end_dt,security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt )
SELECT   end_dt,security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id,start_dt
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security`.`ods_overseas_security`
		EXCEPT
		SELECT   security_id,security_type_en,security_type_cn,security_status,prid,security_name,entity_name,security_domicile,cusip,isin_code,lipper_code,reuters_instrument_code,refinitiv_perm_id,esdid,sedol,ticker_symbol,exchange,security_inactive_reason_cn,security_inactive_reason_en,last_updated_date,last_updated_by,change_detial,change_reason,change_history,comments_1,comments_2,comments_3,comments_4,comments_5,replacement_security_identifier,replacement_security_id
		FROM `security_his`.`dwd_overseas_security`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_overseas_security` AS tb1 
	ON tb0.`security_id` =tb1.`security_id` AND tb0.`security_type_en`=tb1.`security_type_en`
) AS temp;  """},
              "`security_his`.`dwd_fund_clear_info`": {"update": """INSERT INTO `security_his`.`dwd_fund_clear_info`(   end_dt,security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_,start_dt )
SELECT   update_time AS end_dt,security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_,start_dt
FROM (
	SELECT tb2.*,tb1.update_time
	FROM (
		SELECT   security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_
		FROM `security`.`ods_fund_clear_info`
		EXCEPT
		SELECT   security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_
		FROM `security_his`.`dwd_fund_clear_info`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_fund_clear_info` AS tb1 
	ON tb0.`security_identifier` =tb1.`security_identifier` AND tb0.`fund_short_name_cn`=tb1.`fund_short_name_cn`
	JOIN ( SELECT * FROM `security_his`.`dwd_fund_clear_info` WHERE end_dt = '9999-12-31 00:00:00' ) AS tb2
	ON tb0.`security_identifier` =tb2.`security_identifier` AND tb0.`fund_short_name_cn`=tb2.`fund_short_name_cn`
) AS temp;
 """, "insert": """ INSERT INTO `security_his`.`dwd_fund_clear_info`(   end_dt,security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_,start_dt )
SELECT   end_dt,security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_,start_dt
FROM (
	SELECT tb0.*,'9999-12-31 00:00:00' AS end_dt, tb1.update_time AS start_dt
	FROM (
		SELECT   security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_
		FROM `security`.`ods_fund_clear_info`
		EXCEPT
		SELECT   security_identifier,fund_short_name_cn,liquidation_type,maturity_date,liquidation_start_date,liquidation_end_date,liquidation_reason,fund_investment_type,fund_manager_or_advisor_name_cn,initial_fund_or_not,no_
		FROM `security_his`.`dwd_fund_clear_info`
		WHERE end_dt = '9999-12-31 00:00:00'
	) AS tb0 
	JOIN `security`.`ods_fund_clear_info` AS tb1 
	ON tb0.`security_identifier` =tb1.`security_identifier` AND tb0.`fund_short_name_cn`=tb1.`fund_short_name_cn`
) AS temp; """},
              }

    for key in sqlMap:
        sql = sqlMap[key]
        cursor.execute(sql['update'].replace("\n", " "))
        cursor.execute(sql['insert'].replace("\n", " "))


def adsETLJob(cursor):
    sqlList = ["""INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName
)
SELECT 
1 AS security_type,trim(security_id) AS security_id,trim(prid) AS prid,
CASE WHEN prid = '' OR prid is NULL THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS security_status,
ticker_symbol AS code,subscription_ticker_symbol AS subscribe_code,
stock_short_name_cn AS ch_name, stock_short_name_en AS en_name,
"" AS underlying_fund_ch_name,"" AS underlying_fund_en_name,
"" AS stock_code,"" AS stock_name,issuer_name_cn AS ch_issuer,issuer_name_en AS en_issuer,
listing_location_en AS bourse,"" AS p_code, "ListedStock" AS security_type_name,
update_time,create_time,  listing_location_cn AS chineseBourse,listing_location_en AS englishBourse,
'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_listed_stock` ; """,  # WHERE subscription_ticker_symbol IN ('','#N/A') OR subscription_ticker_symbol IS NULL
               """ INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName)
SELECT 
12 AS security_type,trim(security_id) AS security_id,trim(prid) AS prid,
CASE WHEN prid = '' OR prid is NULL THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS security_status,
ticker_symbol AS code,subscription_ticker_symbol AS subscribe_code,
issuer_name_cn AS ch_name, issuer_name_en AS en_name,
"" AS underlying_fund_ch_name,"" AS underlying_fund_en_name,
"" AS stock_code,"" AS stock_name,issuer_name_cn AS ch_issuer,issuer_name_en AS en_issuer,
listing_location_en AS bourse,"" AS p_code, "ListedStock" AS security_type_name,
update_time,create_time,  listing_location_cn AS chineseBourse,listing_location_en AS englishBourse,
'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_listed_stock` 
WHERE subscription_ticker_symbol NOT IN ('','#N/A') AND subscription_ticker_symbol IS NOT NULL ;""",
               """INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName)
SELECT 
2 AS security_type,trim(security_id) AS security_id,trim(prid) AS prid,
CASE WHEN prid = '' OR prid is NULL THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS  security_status,
security_identifier AS code, "" AS subscribe_code,security_short_name_cn AS ch_name, security_short_name_en AS en_name,
"" AS underlying_fund_ch_name,"" AS underlying_fund_en_name,
"" AS stock_code,"" AS stock_name,
issuer_name_cn AS ch_issuer,issuer_name_en AS en_issuer,
listing_location_en AS bourse,"" AS p_code, "PublicOfferingBond" AS security_type_name,
update_time,create_time, 
listing_location_cn AS chineseBourse,listing_location_en AS englishBourse,'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_public_offering_bonds`; """,
               """INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName)
SELECT 
3 AS security_type,trim(security_id) AS security_id,trim(prid) AS prid,
CASE WHEN prid = '' OR prid is NULL THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS  security_status,
security_identifier AS code, "" AS subscribe_code,security_short_name_cn AS ch_name, security_short_name_en AS en_name,
"" AS underlying_fund_ch_name,"" AS underlying_fund_en_name,
ticker_symbol AS stock_code,stock_short_name_en AS stock_name,
issuer_name_cn AS ch_issuer,issuer_name_en AS en_issuer,
listing_location_cn AS bourse,"" AS p_code, "Convertible" AS security_type_name,
update_time,create_time, 
listing_location_cn AS chineseBourse,listing_location_en AS englishBourse,stock_short_name_cn AS chineseStockName,stock_short_name_en AS englishStockName
FROM `security`.`ods_convertible_bonds`; """,
               """ INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName)
SELECT 
4 AS security_type,trim(security_id) AS security_id,trim(prid) AS prid,
CASE WHEN prid = '' OR prid is NULL THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS  security_status,
security_identifier AS code, subscription_security_identifier AS subscribe_code,fund_short_name_cn AS ch_name, fund_short_name_en AS en_name,
"" AS underlying_fund_ch_name,"" AS underlying_fund_en_name,
"" AS stock_code,"" AS stock_name,fund_manager_or_advisor_name_cn AS ch_issuer,fund_manager_or_advisor_name_en AS en_issuer,
listing_location_cn AS bourse,"" AS p_code, "PublicOfferingFund" AS security_type_name,update_time,create_time, 
listing_location_cn AS chineseBourse,listing_location_en AS englishBourse,'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_public_offering_fund` ;""", # WHERE subscription_security_identifier IN ('','#N/A') OR subscription_security_identifier IS NULL
               """INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName)
SELECT 
12 AS security_type,trim(security_id) AS security_id,trim(prid) AS prid,
CASE WHEN prid = '' OR prid is NULL THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS  security_status,security_identifier AS code, 
subscription_security_identifier AS subscribe_code,fund_short_name_cn AS ch_name, fund_short_name_en AS en_name,
"" AS underlying_fund_ch_name,"" AS underlying_fund_en_name,"" AS stock_code,"" AS stock_name,
fund_manager_or_advisor_name_cn AS ch_issuer,fund_manager_or_advisor_name_en AS en_issuer,
listing_location_cn AS bourse,"" AS p_code, "NewStock" AS security_type_name, update_time,create_time, 
listing_location_cn AS chineseBourse,listing_location_en AS englishBourse,'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_public_offering_fund`
WHERE subscription_security_identifier NOT IN ('','#N/A') AND subscription_security_identifier IS NOT NULL ; """,
               """INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName )
SELECT 
5 AS security_type,trim(security_id) AS security_id,trim(prid) AS prid,
CASE WHEN prid = '' OR prid is NULL THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS  security_status,"" AS code, 
"" AS subscribe_code,wealth_management_product_name_cn AS ch_name, wealth_management_product_name_en AS en_name,
"" AS underlying_fund_ch_name,"" AS underlying_fund_en_name,"" AS stock_code,"" AS stock_name,"" AS ch_issuer,"" AS en_issuer,
"" AS bourse,"" AS p_code, "FinancingProduct" AS security_type_name,update_time,create_time, 
'' AS chineseBourse,'' AS englishBourse,'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_wealth_management_product`; """,
               """INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName)
SELECT DISTINCT 
6 AS security_type,trim(security_id_scheme) AS security_id,trim(prid_scheme) AS prid,
CASE WHEN prid_scheme is NULL OR trim(prid_scheme) = '' THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS  security_status,"" AS code, 
"" AS subscribe_code,hongkong_mpf_scheme_name_cn AS ch_name, hongkong_mpf_scheme_name_en AS en_name,
"" AS underlying_fund_ch_name,"" AS underlying_fund_en_name,"" AS stock_code,"" AS stock_name,"" AS ch_issuer,"" AS en_issuer,
"" AS bourse,"" AS p_code, "HK_MPF" AS security_type_name,update_time,create_time, 
'' AS chineseBourse,'' AS englishBourse,'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_hk_mpf`; """,
               """INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName )
SELECT DISTINCT 
11 AS security_type,trim(security_id_cf) AS security_id,trim(cf_security_linked_entity_prid) AS prid,
CASE WHEN cf_security_linked_entity_prid is NULL OR trim(cf_security_linked_entity_prid) = '' THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS  security_status,
'' AS code, '' AS subscribe_code,hongkong_mpf_cf_name_cn AS ch_name, hongkong_mpf_cf_name_en AS en_name,
underlying_fund_name_cn AS underlying_fund_ch_name,underlying_fund_name_en AS underlying_fund_en_name,
'' AS stock_code,'' AS stock_name,'' AS ch_issuer,'' AS en_issuer,'' AS bourse,
CONCAT(trim(IF(security_id_scheme is null,"",security_id_scheme)),'-6-',trim(IF(prid_scheme is null,"",prid_scheme)))  AS p_code, 
"HK_MPF_sub" AS security_type_name,update_time,create_time, 
'' AS chineseBourse,'' AS englishBourse,'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_hk_mpf`; """,
               """ INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName  )
SELECT DISTINCT 
7 AS security_type,trim(security_id) AS security_id,trim(prid) AS prid,
CASE WHEN prid = '' OR prid is NULL THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS  security_status,"" AS code, 
'' AS subscribe_code,macao_non_mandatory_central_provident_fund_name_cn AS ch_name, macao_non_mandatory_central_provident_fund_name_en AS en_name,
'' AS underlying_fund_ch_name,'' AS underlying_fund_en_name,'' AS stock_code,'' AS stock_name,
fund_manager_or_advisor_name_cn AS ch_issuer,fund_manager_or_advisor_name_en AS en_issuer,
'' AS bourse,'' AS p_code, "CentralFundMacau" AS security_type_name,update_time,create_time, 
'' AS chineseBourse,'' AS englishBourse,'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_macao_central_fund`;""",
               """ INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName  )
SELECT DISTINCT 
8 AS security_type,trim(security_id) AS security_id,trim(prid) AS prid,
CASE WHEN prid = '' OR prid is NULL THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS  security_status,"" AS code, 
'' AS subscribe_code,entity_name_cn AS ch_name, entity_name_en AS en_name,
'' AS underlying_fund_ch_name,'' AS underlying_fund_en_name,'' AS stock_code,'' AS stock_name,'' AS ch_issuer,'' AS en_issuer,
'' AS bourse,'' AS p_code, "UnlistedEquity" AS security_type_name,update_time,create_time, 
'' AS chineseBourse,'' AS englishBourse,'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_unlisted_stock`;""",
               """INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName  )
SELECT DISTINCT 
9 AS security_type,trim(security_id) AS security_id,trim(prid) AS prid,
CASE WHEN prid = '' OR prid is NULL THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS  security_status,
product_code AS code, '' AS subscribe_code,product_name_cn AS ch_name, product_name_en AS en_name,
'' AS underlying_fund_ch_name,'' AS underlying_fund_en_name,'' AS stock_code,'' AS stock_name,
'' AS ch_issuer,'' AS en_issuer,'' AS bourse,'' AS p_code, "OtherSecurity" AS security_type_name,update_time,create_time, 
'' AS chineseBourse,'' AS englishBourse,'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_others_security`; """,
               """INSERT into `security`.`ads_security_msg_all`(
security_type,security_id,prid,security_status,code,subscribe_code,ch_name,en_name,
underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,p_code,security_type_name,update_time,create_time,
chineseBourse,englishBourse,chineseStockName,englishStockName   )
SELECT DISTINCT 
10 AS security_type,trim(security_id) AS security_id,trim(prid) AS prid,
CASE WHEN prid = '' OR prid is NULL THEN 3 WHEN LOWER(trim(security_status)) = 'inactive' THEN 2 ELSE 1 END AS  security_status,'' AS code, 
'' AS subscribe_code, '' AS ch_name, security_name AS en_name,'' AS underlying_fund_ch_name,'' AS underlying_fund_en_name,
'' AS stock_code,'' AS stock_name,'' AS ch_issuer,'' AS en_issuer,security_domicile AS bourse,'' AS p_code, 'OverseasSecurity' AS security_type_name,
update_time,create_time, security_domicile AS chineseBourse,security_domicile AS englishBourse,'' AS chineseStockName,'' AS englishStockName
FROM `security`.`ods_overseas_security`; """]
    for sql in sqlList:
        cursor.execute(sql)


def adsHisJob(cursor):
    sqlList = [""" INSERT INTO `security_his`.`ads_security_msg_his`(
start_dt,end_dt,security_type,security_id,prid,p_code,security_status,code,subscribe_code,ch_name,
en_name,underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,chineseBourse,englishBourse,chineseStockName,englishStockName,security_type_name,update_time,create_time,mark )
SELECT 
start_dt,end_dt,security_type,security_id,prid,p_code,security_status,code,subscribe_code,ch_name,
en_name,underlying_fund_ch_name,underlying_fund_en_name,stock_code,stock_name,ch_issuer,en_issuer,
bourse,chineseBourse,englishBourse,chineseStockName,englishStockName,security_type_name,update_time,create_time,mark
FROM (
SELECT  
ads.update_time AS start_dt, '9999-12-31 00:00:00' AS end_dt,
ads.security_type,ads.security_id,ads.prid,ads.p_code,ads.security_status,ads.code,ads.subscribe_code,ads.ch_name,
ads.en_name,ads.underlying_fund_ch_name,ads.underlying_fund_en_name,ads.stock_code,ads.stock_name,ads.ch_issuer,ads.en_issuer,
ads.bourse,ads.chineseBourse,ads.englishBourse,ads.chineseStockName,ads.englishStockName,ads.security_type_name,
IF(his.security_type IS NULL,'A','U' ) AS mark,ads.update_time,ads.create_time
FROM `security`.`ads_security_msg_all` AS ads 
LEFT JOIN (SELECT * FROM `security_his`.`ads_security_msg_his` AS his WHERE his.end_dt = '9999-12-31') AS his
ON his.security_type=ads.security_type AND his.security_id = ads.security_id AND his.prid=ads.prid
WHERE his.update_time<>ads.update_time OR his.end_dt IS NULL
UNION 
SELECT  his.start_dt AS start_dt,ads.update_time AS end_dt,
his.security_type,his.security_id,his.prid,his.p_code,his.security_status,his.code,his.subscribe_code,his.ch_name,
his.en_name,his.underlying_fund_ch_name,his.underlying_fund_en_name,his.stock_code,his.stock_name,his.ch_issuer,his.en_issuer,
his.bourse,his.chineseBourse,his.englishBourse,his.chineseStockName,his.englishStockName,his.security_type_name,
his.mark,his.update_time,his.create_time
FROM `security_his`.`ads_security_msg_his` AS his
JOIN `security`.`ads_security_msg_all` AS ads
ON his.security_type=ads.security_type AND his.security_id = ads.security_id AND his.prid=ads.prid AND his.end_dt = '9999-12-31'
WHERE his.update_time<>ads.update_time
) AS temp; """]
    for sql in sqlList:
        cursor.execute(sql)


if __name__ == '__main__':
    fileList = download(host="cnshaappuwv016", port=21, user="test1", pwd="test1")
    cursor = DorisConnection.getCursor(host="10.158.16.244", user="root", password="", database="security")
    loadData = ExcelToDoris(cursor=cursor)
    for path in fileList:
        print(path)
        temp = datetime.strptime(str(path).split("/")[-1].split(".")[0], '%Y%m%d%H%M%S')
        loadData.truncateTable()
        loadData.loadData(filePath=path, upsertTime=temp.strftime("%Y-%m-%d %H:%M:%S"))
        etlSecurityData(cursor=cursor)
        updateSecurityStatus(cursor=cursor)
        etlSecurityHisData(cursor=cursor)
        adsETLJob(cursor=cursor)
        adsHisJob(cursor=cursor)

    DorisConnection.close()
