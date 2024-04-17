import datetime
import pyodbc
import pymysql

syncData = {
    "procurement_all.ods_fin_procurement_project_data_hour_ei": {"table": "ProcurementCentreDataPlatform.dbo.ads_procurement_project_data",
                                                        "ignoreDeleted": True},
    "procurement_all.ods_fin_project_plan_list_hour_ei": {"table": "ProcurementCentreDataPlatform.dbo.ads_project_plan_list", "ignoreDeleted": True},
    "procurement_all.ods_fin_project_plan_part_breakdow_hour_ei": {"table": "ProcurementCentreDataPlatform.dbo.ads_project_plan_part_breakdow",
                                                          "ignoreDeleted": True},
    "procurement_all.ods_fin_vendor_selection_plan_list_hour_ei": {"table": "ProcurementCentreDataPlatform.dbo.ads_vendor_selection_plan_list",
                                                          "ignoreDeleted": False},
    "procurement_all.ods_fin_vs_plan_part_breakdown_hour_ei": {"table": "ProcurementCentreDataPlatform.dbo.ads_vs_plan_part_breakdown",
                                                      "ignoreDeleted": False},
    "procurement_all.ods_fin_vendor_selection_result_hour_ei": {"table": "ProcurementCentreDataPlatform.dbo.ads_vendor_selection_result",
                                                       "ignoreDeleted": False},
    "procurement_all.ods_fin_vendor_selection_result_foreign_hour_ei": {"table": "ProcurementCentreDataPlatform.dbo.ads_vendor_selection_result_foreign",
                                                               "ignoreDeleted": False},
}
batchSize = 1000

conn_obj = pymysql.connect(
    host='10.158.15.148',  # MySQL服务端的IP地址
    port=6030,  # MySQL默认PORT地址(端口号)
    user='admin_user',  # 用户名
    password='6a!F@^ac*jBHtc7uUdxC',  # 密码,也可以简写为passwd
    database='procurement_all',  # 库名称,也可以简写为db
    charset='utf8'  # 字符编码
)
# 产生获取命令的游标对象
# cursor = conn_obj.cursor()  # 括号内不写参数,数据是元组套元组
source = conn_obj.cursor(
    cursor=pymysql.cursors.DictCursor
)  # 括号内写参数,数据会处理成字典形式

_CONNECTION_STRING = (
    'DRIVER={_driver};SERVER={_db_server};'
    'DATABASE={_db_name};UID={_db_uid};PWD={_db_pwd};'
    'TrustServerCertificate=YES;'
).format(
    # _driver='{ODBC Driver 18 for SQL Server}',
    # _driver='{ODBC Driver 17 for SQL Server}',
    _driver='{SQL Server}',
    _db_server='CNCSQLPWV5028,1800',
    _db_name='ProcurementCentreDataPlatform',
    _db_uid='ProcurementCentreWriter',
    _db_pwd='b5OwBAhhRhmIqr4qfJ(F'
)

target = pyodbc.connect(_CONNECTION_STRING)  # autocommit=True

today = datetime.datetime.now()
update_time = today.strftime("%Y-%m-%d %H:%M:%S")

for tableName in syncData:
    columnMap = {}
    source.execute("show full columns from {}".format(tableName))
    res = source.fetchall()
    for mid in res:
        tmp = {"key": mid["Comment"], "type": mid["Type"]}
        columnMap[mid["Field"]] = tmp

    meta_data = columnMap
    sql = "SELECT {column} FROM {table}".format(column=",".join(list(meta_data.keys())), table=tableName)
    if syncData[tableName]["ignoreDeleted"]:
        sql += " WHERE deletion_time IS NULL AND is_deleted = 'No' "
    source.execute(sql)
    data = source.fetchall()

    if len(data) == 0:
        print(update_time, "{} do not have data".format(tableName))
    else:

        table = syncData[tableName]["table"]
        meta_map = meta_data
        deleteSql = " DELETE FROM  {}".format(table)

        cache = "insert into  {}( ".format(table)
        for columnName in meta_map:
            cache += str(meta_map[columnName]["key"]).replace(" ", "") + ","
        cache = cache[:-1] + ") values "

        truncateFlag = True
        count = 0
        sql = ""
        for mid in data:
            sql += "("

            for source_key in meta_map:
                source_key = source_key
                target_column_name = str(meta_map[source_key]["key"])
                target_column_type = str(meta_map[source_key]["type"]).lower()

                columnData = 'null' if mid[source_key] is None else str(mid[source_key])

                if target_column_name == "IsDeleted":
                    columnData = "'No'" if columnData.lower() in ["null", "false", "no"] else "'Yes'"

                elif "decimal" not in target_column_type and "int" not in target_column_type and "null" not in columnData:
                    columnData = "N'" + columnData.replace("'", "''") + "'"

                sql += columnData + ","

            sql = sql[:-1] + " ),"

            count += 1
            if count % batchSize == 0:
                if truncateFlag:
                    target.execute(deleteSql)
                    truncateFlag = False
                sql = cache + sql[:-1]
                res = target.execute(sql).rowcount
                if res < batchSize:
                    print(len(data), res, sql)
                    target.rollback()
                else:
                    sql = ""
                    target.commit()

        if truncateFlag:
            target.execute(deleteSql)
            truncateFlag = False

        if sql != "":
            sql = cache + sql[:-1]
            res = target.execute(sql).rowcount
            if res < (count % batchSize):
                print(len(data), res, sql)
                target.rollback()
            else:
                target.commit()

source.close()
target.close()