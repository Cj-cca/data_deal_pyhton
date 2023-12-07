import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from urllib.parse import quote_plus as urlquote

SrcSelectFields = ("CountryCode as country_code,PowerOfficeCode as power_office_code,PowerGroupCode as "
                   "power_group_code,ServiceLineCode as service_line_code,ServiceLineDesc as service_line_desc")
tarSelectFields = "country_code,power_office_code,power_group_code,service_line_code,service_line_desc"
SrcJointIndex = "CountryCode,PowerOfficeCode,PowerGroupCode,ServiceLineCode"
TarJointIndex = "country_code,power_office_code,power_group_code,service_line_code"


def run(src_engine, tar_engine, src_table_name, tar_table_name):
    src_conn = src_engine.connect()
    tar_conn = tar_engine.connect()
    src_count_result = src_conn.execute(text(
        f"select count(*)as cnt from {src_table_name}"))
    # 获取第一条元素的第一个字段
    src_count = src_count_result.fetchone()[0]
    tar_count_result = tar_conn.execute(text(
        f"select count(*)as cnt from {tar_table_name}"))
    # 获取第一条元素的第一个字段
    tar_count = tar_count_result.fetchone()[0]
    data_count = src_count if src_count > tar_count else tar_count
    print(f"doris数据量查询完成, 原表数量: {src_count},目标表数量: {tar_count}")
    gap = 10000
    for i in range(0, data_count, gap):
        end = i + gap
        if end > data_count:
            end = data_count
        src_sql = text(
            f"select {SrcSelectFields} from (select ROW_NUMBER() OVER(Order by {SrcJointIndex}) AS rowNumber,* from {src_table_name}) as tbl where tbl.RowNumber >{i} and tbl.RowNumber <={end}")
        tar_sql = text(
            f"select {tarSelectFields} from (select ROW_NUMBER() OVER(Order by {TarJointIndex}) AS rowNumber,* from {tar_table_name}) as tbl where tbl.RowNumber >{i} and tbl.RowNumber <={end}")
        print("开始比较批次数据")
        df1 = pd.read_sql(src_sql, src_engine)
        df2 = pd.read_sql(tar_sql, tar_engine)
        # Merge the DataFrames and identify differing rows
        merged = df1.merge(df2, indicator=True, how='outer')
        diff_df = merged[merged['_merge'] != 'both']
        if diff_df.size == 0:
            print("数据一致")
        else:
            print("Differing data:")
            print(diff_df)


if __name__ == '__main__':
    srcTableName = "ServiceLine"
    tarTableName = "ods_hr_manual_service_line"
    srcEngine = create_engine('mysql+pymysql://root@10.158.34.175:9030/StaffBank')
    tarEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/staff_bank"
    )
    run(srcEngine, tarEngine, srcTableName, tarTableName)
