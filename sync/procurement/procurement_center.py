import json
import datetime
import pymysql
import requests

conn_obj = pymysql.connect(
    host='10.158.35.241',  # MySQL服务端的IP地址
    port=9030,  # MySQL默认PORT地址(端口号)
    user='admin_user',  # 用户名
    password='6a!F@^ac*jBHtc7uUdxC',  # 密码,也可以简写为passwd
    database='procurement_all',  # 库名称,也可以简写为db
    charset='utf8'  # 字符编码
)

# 产生获取命令的游标对象
# cursor = conn_obj.cursor()  # 括号内不写参数,数据是元组套元组
cursor = conn_obj.cursor(
    cursor=pymysql.cursors.DictCursor
)  # 括号内写参数,数据会处理成字典形式

# uat环境
syncData = {
    # 1-1
    "procurement_all.ods_fin_procurement_project_data_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/project-overall-list",
        "depend_on": ""},
    # 1-2
    "procurement_all.ods_fin_project_plan_list_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/project-plan-list",
        "depend_on": ""},
    # 1-3
    "procurement_all.ods_fin_project_plan_part_breakdow_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/project-plan-part-breakdown",
        "depend_on": ""},
    # 1-4
    "procurement_all.ods_fin_project_plan_budget_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/project-plan-budget",
        "depend_on": ""},
    # 2-1
    "procurement_all.ods_fin_vendor_selection_plan_list_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/vendor-selection-plan-list",
        "depend_on": ""},
    # 2-2
    "procurement_all.ods_fin_vs_plan_part_breakdown_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/vs-plan-part-breakdown",
        "depend_on": ""},
    # 2-3
    "procurement_all.ods_fin_vendor_selection_result_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/vendor-selection-result",
        "depend_on": ""},
    # 2-4
    "procurement_all.ods_fin_vendor_selection_result_foreign_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/vendor-selection-result-foreign",
        "depend_on": ""},
    # 2-5
    "procurement_all.ods_fin_vs_to_procurement_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/vendor-selection-procurement",
        "depend_on": ""},
    # 6-1
    "procurement_all.ods_fin_access_control_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/access-control",
        "depend_on": ""},
    # 6-2
    "procurement_all.ods_fin_fy_exchange_rate_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/fy-exchange-rate",
        "depend_on": ""},
    # 6-3
    "procurement_all.ods_fin_dim_source_info_hour_ei": {
        "url": "https://procurementcentre.asia.pwcinternal.com/dataasync/api/data-sync/categorys",
        "depend_on": ""}

}

deleteSql = " DELETE FROM {} WHERE VendorSelectionCode IN ( "
needDelete = False

today = datetime.datetime.now()
delta = datetime.timedelta(days=1)
search_start = (today - delta).strftime("%Y-%m-%d 00:00:00")
search_end = today.strftime("%Y-%m-%d 23:59:59")

params = {"beginTime": search_start, "endTime": search_end}
# params = {"beginTime": '', "endTime": ''}
# data = requests.get(url=syncData["procurement_centre.ods_vendor_selection_plan_list"]["url"], params=params,
#                     verify=False).text
data = ""
if len(data) > 0:
    data = json.loads(data)
    data = data["data"] if "data" in data else []
    for mid in data:
        needDelete = True
        deleteSql += "'" + str(mid["VendorSelectionCode"]).replace("'", "\\'") + "',"

    deleteSql = deleteSql[:-1] + ") "

for table in syncData:

    columnMap = {}
    cursor.execute("show full columns from {}".format(table))
    res = cursor.fetchall()
    for mid in res:
        tmp = {"key": mid["Comment"], "type": mid["Type"]}
        columnMap[mid["Field"]] = tmp

    columnsMeta = columnMap
    cache = "insert into  {}( `".format(table)
    for columnName in columnsMeta.keys():
        cache += columnName + "`,`"
    cache = cache[0:len(cache) - 2] + ") values "

    url = syncData[table]["url"]

    data = requests.get(url=url, params=params, verify=False).text
    if len(data) > 0:
        # print(params, table)
        data = json.loads(data)
        data = data["data"] if "data" in data else []
    else:
        data = ""

    if data is None:
        data = ""

    if len(data) > 0:
        if "" != syncData[table]["depend_on"] and needDelete:
            print(deleteSql.format(table))
            cursor.execute(deleteSql.format(table))

        columnsData = columnMap.keys()
        sql = ""
        today = datetime.datetime.now()
        update_time = today.strftime("%Y-%m-%d %H:%M:%S")

        for mid in data:
            sql += "("
            for columnName in columnsData:
                if columnName in ("update_time", "create_time"):
                    sql += "'" + update_time + "',"
                else:
                    metaData = columnMap[columnName]
                    columnData = 'null' if metaData["key"] not in mid or mid[metaData["key"]] is None else str(
                        mid[metaData["key"]]).strip().replace("\\", "\\\\'").replace("'", "\\'")
                    if "DECIMAL" not in metaData["type"] and "INT" not in metaData["type"] and "null" not in columnData:
                        columnData = "'" + columnData + "'"
                    if "DECIMAL" in metaData["type"] or "INT" in metaData["type"]:
                        if columnData == 'null':
                            columnData = '0.0'
                        else:
                            columnData = str(round(float(columnData), 3))
                    sql += columnData + ","
            sql = sql[:-1] + " ),"

        sql = cache + sql[:-1]

        try:
            res = cursor.execute(sql)
            print(f"table：{table},应入数据条数：{len(data)},实际入数据条数：{res}")
            if res < len(data):
                print(table, json.dumps(params), len(data), res)
                print(data)
        except Exception as e:
            print(e)

cursor.close()
