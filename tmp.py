from sqlalchemy import create_engine
from sqlalchemy import text
from urllib.parse import quote_plus as urlquote
import time


def generate_his_sql(data_base, table_change, table_his, table_dwd, fields, unique_keys):
    fields_str = ', '.join(fields)
    select_field = ', '.join(['his.' + field for field in fields])
    joinOnCondition = [f'his.{key} = org.{key}' for key in unique_keys]
    signal_key = unique_keys[0]
    sql_list = []
    get_delete_data_sql = f"""insert into `{data_base}`.`{table_change}`({fields_str},etl_date)
    SELECT {select_field}, CURDATE() as etl_date
    FROM (
        select *
            from `{data_base}`.`{table_his}` where end_date = '9999-12-31'
        ) AS his LEFT JOIN `{data_base}`.`{table_dwd}` AS org
    ON {' AND '.join(joinOnCondition)} where org.{signal_key} IS NULL;
    """
    sql_list.append(get_delete_data_sql)

    # 把修改了的原始的数据更新endTIme
    get_change_data_sql = f"""insert into `{data_base}`.`{table_change}`({fields_str},etl_date)
select {select_field},CURDATE() as etl_date
from(
     SELECT  {fields_str}
	FROM `{data_base}`.`{table_dwd}` EXCEPT SELECT {fields_str}
    FROM `{data_base}`.`{table_his}` WHERE end_date = '9999-12-31'
        ) as org
join (select * from `{data_base}`.`{table_his}` where end_date = '9999-12-31') AS his
    on {' AND '.join(joinOnCondition)};
"""
    sql_list.append(get_change_data_sql)
    return sql_list


def etlHistoryTableData(engine, sqlList):
    conn = engine.connect()
    for sql in sqlList:
        res = conn.execute(text(sql.replace("\n", " ")))
        print(sql, ",执行成功,受影响的行数: ", res.rowcount)


if __name__ == '__main__':
    databasesName = 'talenlink'
    tableName_his = 'ods_tbl_talent_link_original_day_st'
    tableName_dwd = 'ods_tbl_talent_link_original_day_ef'
    tableName_change = 'ods_tbl_talent_link_original_change_day_ef'
    allFields = ["booking_id","staff_id","job_id","employee_id","start_date","end_date","update_date","client_code","job_code","office_code","create_by_date","loading","worker_id","job_id_desc","res_id","date_range"]
    uniqueKey = ['booking_id']
    sqlList = generate_his_sql(databasesName, tableName_change, tableName_his, tableName_dwd, allFields, uniqueKey)
    tarEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/talenlink"
    )
    etlHistoryTableData(tarEngine, sqlList)
