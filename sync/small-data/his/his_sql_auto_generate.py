from sqlalchemy import create_engine
from sqlalchemy import text
from urllib.parse import quote_plus as urlquote
import time


def generate_his_sql(data_base, table_his, table_dwd, fields, unique_keys):
    fields_str = ', '.join(fields)
    select_field = ', '.join(['his.' + field for field in fields])
    joinOnCondition = [f'his.{key} = org.{key}' for key in unique_keys]
    signal_key = unique_keys[0]
    sql_list = []
    get_delete_data_sql = f"""insert into `{data_base}`.`{table_his}`(start_date, {fields_str}, end_date)
    SELECT his.start_date, {select_field}, CURDATE() as end_date
    FROM (
        select *
            from `{data_base}`.`{table_his}` where end_date = '9999-12-31'
        ) AS his LEFT JOIN `{data_base}`.`{table_dwd}` AS org
    ON {' AND '.join(joinOnCondition)} where org.{signal_key} IS NULL;
    """
    sql_list.append(get_delete_data_sql)

    # 把修改了的原始的数据更新endTIme
    get_change_data_sql = f"""insert into `{data_base}`.`{table_his}`(start_date, {fields_str},  end_date)
select his.start_date, {select_field},CURDATE() as end_date
from(
     SELECT  {fields_str}
	FROM `{data_base}`.`{table_dwd}` EXCEPT SELECT {fields_str}
    FROM `{data_base}`.`{table_his}` WHERE end_date = '9999-12-31'
        ) as org
join (select * from `{data_base}`.`{table_his}` where end_date = '9999-12-31') AS his
    on {' AND '.join(joinOnCondition)};
"""
    sql_list.append(get_change_data_sql)

    # 插入新曾和修改了的数据
    get_new_date_sql = f"""insert into `{data_base}`.`{table_his}`(start_date, {fields_str}, end_date)
select curdate() as start_date,org.*,'9999-12-31' as end_date
from(
     SELECT  {fields_str}
	FROM `{data_base}`.`{table_dwd}` EXCEPT SELECT {fields_str}
    FROM `{data_base}`.`{table_his}` WHERE end_date = '9999-12-31'
        ) as org
left join (select * from `{data_base}`.`{table_his}` where end_date = '9999-12-31') AS his
    on {' AND '.join(joinOnCondition)}
where his.{signal_key} is null;"""
    sql_list.append(get_new_date_sql)
    return sql_list


def etlHistoryTableData(engine, sqlList):
    conn = engine.connect()
    for sql in sqlList:
        res = conn.execute(text(sql.replace("\n", " ")))
        print(sql, ",执行成功,受影响的行数: ", res.rowcount)


if __name__ == '__main__':
    databasesName = 'pwc_mdm'
    tableName_his = 'ods_fin_vw_firm_user_role_day_ef'
    tableName_dwd = 'dwd_fin_vw_firm_user_role_day_st'
    allFields = ['id',
                 'staff_id',
                 'staff_name',
                 'role_code',
                 'role_desc',
                 'office_code',
                 'group_code',
                 'group_desc',
                 'los_code',
                 'los_desc',
                 'country_code',
                 'ou_status',
                 'da_termination_date']
    uniqueKey = ['id']
    sqlList = generate_his_sql(databasesName, tableName_his, tableName_dwd, allFields, uniqueKey)
    tarEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/pwc_mdm"
    )
    start = time.time()
    etlHistoryTableData(tarEngine, sqlList)
    cost_time = round(time.time() - start, 2)
    print("")
