import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

# resultColumn = ["Worker_ID", "Certification_Reference_ID", "Issued_Date", "Certification_Name", "Examination_Date",
#                 "Examination_Score", "Expiration_Date"]
# combinationColumns = ["Certification_Reference_ID", "Issued_Date", "Certification_Name", "Examination_Date",
#                       "Examination_Score", "Expiration_Date"]
#
# groupField = "Work_ID"
# sortField = "Issued_Date"
# tarTable = "HRWorkers_Certification_ads"
# selectHisTable = "HRWorkers_Certification_dwd_his"
# tarEngine = create_engine('mysql+pymysql://root@10.158.16.244:9030/WorkDayStage')


resultColumn = ['Worker_ID',
                'First_Year_Attended',
                'Degree',
                'Is_Highest_Level_of_Education',
                'Last_Year_Attended',
                'School_ID',
                'School_Name',
                'Education_Country',
                'Degree_Receiving_Date',
                'Field_Of_Study']

combinationColumns = ['First_Year_Attended',
                      'Degree',
                      'Is_Highest_Level_of_Education',
                      'Last_Year_Attended',
                      'School_ID',
                      'School_Name',
                      'Education_Country',
                      'Degree_Receiving_Date',
                      'Field_Of_Study']

groupField = "Worker_ID"
sortField = "First_Year_Attended"
tarTable = "HRWorkers_Education_ads"
selectHisTable = "HRWorkers_Education_dwd_his"
tarEngine = create_engine('mysql+pymysql://root@10.158.16.244:9030/WorkDayStage')


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
    df = pd.read_sql(text(f"select {selectField} from {selectHisTable} where endDate = '9999-12-31'"),
                     conn)
    result = df.groupby(f"{groupField}").apply(concat_sorted)
    data = result.reset_index(drop=True)
    truncateTable(tarEngine, tarTable)
    insert_count = data.to_sql(tarTable, tarEngine, if_exists='append', index=False)
    print(f"{tarTable}数据插入成功，受影响行数：", insert_count)
