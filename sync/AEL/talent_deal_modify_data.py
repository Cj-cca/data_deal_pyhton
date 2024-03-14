import pymysql
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from urllib.parse import quote_plus as urlquote


def generate_his_sql(data_base, table_his, table_dwd, fields, unique_keys):
    fields_str = ', '.join(fields)
    select_field = ', '.join(['his.' + field for field in fields])
    joinOnCondition = [f'his.{key} = org.{key}' for key in unique_keys]
    signal_key = unique_keys[0]
    sql_map = {}
    # 把删除了的数据在历史表里面更新endTime
    get_delete_data_sql = f"""insert into `{data_base}`.`{table_his}`(start_dt, {fields_str}, end_dt)
SELECT his.start_dt, {select_field}, CURDATE() as end_dt
FROM (
    select *
        from `{data_base}`.`{table_his}` where end_dt = '9999-12-31'
    ) AS his LEFT JOIN `{data_base}`.`{table_dwd}` AS org
ON {' AND '.join(joinOnCondition)} where org.{signal_key} IS NULL;
"""
    sql_map['delete_data'] = get_delete_data_sql

    # 把修改了的数据在历史表中更新endTime
    get_change_data_sql = f"""insert into `{data_base}`.`{table_his}`(start_dt, {fields_str},  end_dt)
select his.start_dt, {select_field},CURDATE() as end_dt
from(
     SELECT  {fields_str}
	FROM `{data_base}`.`{table_dwd}` EXCEPT SELECT {fields_str}
    FROM `{data_base}`.`{table_his}` WHERE end_dt = '9999-12-31'
        ) as org
join (select * from `{data_base}`.`{table_his}` where end_dt = '9999-12-31') AS his
    on {' AND '.join(joinOnCondition)};
"""
    sql_map['update_data'] = get_change_data_sql

    # 插入新曾和修改了的数据
    get_new_date_sql = f"""insert into `{data_base}`.`{table_his}`(start_dt, {fields_str}, end_dt)
select curdate() as start_dt,org.*,'9999-12-31' as end_dt
from(
     SELECT  {fields_str}
	FROM `{data_base}`.`{table_dwd}` EXCEPT SELECT {fields_str}
    FROM `{data_base}`.`{table_his}` WHERE end_dt = '9999-12-31'
        ) as org
left join (select * from `{data_base}`.`{table_his}` where end_dt = '9999-12-31') AS his
    on {' AND '.join(joinOnCondition)}
where his.{signal_key} is null;"""
    sql_map['insert_data'] = get_new_date_sql
    return sql_map


def sql_list_exec(engine, sql_list):
    conn = engine.connect()
    for sql in sql_list:
        res = conn.execute(text(sql.replace("\n", " ")))
        print(sql, ",执行成功,受影响的行数: ", res.rowcount)


def select_data(src_engine, exec_sql):
    sql = text(exec_sql)
    srcConn = src_engine.connect()
    dataFrame = pd.read_sql(sql, srcConn)
    print(f"数据查询完成，数据条数为：{dataFrame.size}")
    return ','.join(dataFrame['booking_id'].astype(str))


def mysql_delete_data(exec_sql):
    print("开始创建连接")
    connection = pymysql.connect(host="10.157.112.167",
                                 port=3306,
                                 user="oats_talentlink",
                                 password="Fo@tI%Vwc(AO",
                                 db="AEL",
                                 charset="utf8")
    cursor = connection.cursor()
    cursor.execute(exec_sql)
    connection.commit()
    results = cursor.fetchall()
    for row in results:
        print(row)
    cursor.close()
    connection.close()


if __name__ == '__main__':
    doris_databasesName = 'talenlink'
    doris_tableName_his = 'ods_tbl_talent_link_original_day_st'
    doris_tableName_dwd = 'ods_tbl_talent_link_original_day_ef'
    mysql_AEL_table = "AEL.ODS_ADVISORY_TALENT_LINK"
    doris_AEL_table = "advisory_engagement_lifecycle.ods_advisory_talent_link_update_field_and_key"
    allFields = ["booking_id", "staff_id", "job_id", "employee_id", "start_date", "end_date", "update_date",
                 "client_code", "job_code", "office_code", "create_by_date", "loading", "worker_id", "job_id_desc",
                 "res_id", "date_range"]
    uniqueKey = ['booking_id']
    sqlMap = generate_his_sql(doris_databasesName, doris_tableName_his, doris_tableName_dwd, allFields, uniqueKey)
    tarEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/talenlink"
    )
    # 将删除和修改了的数据在历史表中更新结束时间
    sql_list_exec(tarEngine, [sqlMap['delete_data'], sqlMap['update_data']])
    # 获取删除和修改了的booking_id，并在doris和mysql中删除
    select_sql = f"select booking_id from {doris_tableName_his} where end_dt = CURDATE()"
    booking_id_lists_str = select_data(tarEngine, select_sql)
    doris_delete_sql = f"delete from {doris_AEL_table} where booking_id in ({booking_id_lists_str})"
    mysql_delete_sql = f"delete from {mysql_AEL_table} where booking_id in ({booking_id_lists_str})"
    sql_list_exec(tarEngine, [doris_delete_sql])
    print("历史表中删除和修改的了的原始数据删除成功")
    mysql_delete_data(mysql_delete_sql)
    # 将新增和修改之后的数据写入历史表中
    sql_list_exec(tarEngine, [sqlMap['insert_data']])
