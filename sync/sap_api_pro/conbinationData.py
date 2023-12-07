import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from urllib.parse import quote_plus as urlquote

# resultColumn = ["worker_id", "certification_reference_id", "issued_date", "certification_name", "examination_date",
#                 "examination_score", "expiration_date"]
# combinationColumns = ["certification_reference_id", "issued_date", "certification_name", "examination_date",
#                       "examination_score", "expiration_date"]
#
# groupField = "worker_id"
# sortField = "issued_date"
# tarTable = "ads_hr_workers_certification_day_ef"
# selectHisTable = "dwd_hr_workers_certification_day_st"
# tarEngine = create_engine(
#         f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/work_day_stage"
#     )


resultColumn = ['worker_id',
                'first_year_attended',
                'degree',
                'is_highest_level_of_education',
                'last_year_attended',
                'school_id',
                'school_name',
                'education_country',
                'degree_receiving_date',
                'field_of_study']

combinationColumns = ['first_year_attended',
                      'degree',
                      'is_highest_level_of_education',
                      'last_year_attended',
                      'school_id',
                      'school_name',
                      'education_country',
                      'degree_receiving_date',
                      'field_of_study']

groupField = "worker_id"
sortField = "first_year_attended"
tarTable = "ads_hr_workers_education_day_ef"
selectHisTable = "dwd_hr_workers_education_day_st"
tarEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/work_day_stage"
    )


# 定义自定义函数，将原始数据的多条合并为一条
def concat_sorted(group):
    work_id = group.name
    # 对分组内的数据进行排序
    tmp_dataframe = pd.DataFrame(columns=resultColumn)
    group_sorted = group.sort_values(f"{sortField}")
    data_list = [work_id]
    for combinationColumn in combinationColumns:
        data_list.append(','.join(
            [elem if elem != '' else 'None' for elem in group_sorted[f"{combinationColumn}"].tolist()]))
    tmp_dataframe.loc[len(tmp_dataframe.index)] = data_list
    return tmp_dataframe


def truncateTable(engine, table_name):
    tarConn = engine.connect()
    tarConn.execute(text(f"truncate table {table_name}"))
    tarConn.close()
    print("table truncate is complete")


if __name__ == '__main__':
    conn = tarEngine.connect()
    selectField = ','.join(resultColumn)
    df = pd.read_sql(text(f"select {selectField} from {selectHisTable} where end_date = '9999-12-31'"),
                     conn)
    result = df.groupby(f"{groupField}").apply(concat_sorted)
    data = result.reset_index(drop=True)
    truncateTable(tarEngine, tarTable)
    insert_count = data.to_sql(tarTable, tarEngine, if_exists='append', index=False)
    print(f"{tarTable}数据插入成功，受影响行数：", insert_count)
