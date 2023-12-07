import pandas as pd
import time
from sqlalchemy import text
from sqlalchemy import create_engine


def run(srcEngine, tarEngine, table):
    sql = text(f"select * from {table}")
    srcConn = srcEngine.connect()
    tarConn = tarEngine.connect()
    tarConn.execute(text(f"truncate table {table}"))
    dataFrame = pd.read_sql(sql, srcConn)
    dataFrame.to_sql(table, tarEngine, if_exists='append', index=False)
    print(table, "-同步完成")


if __name__ == '__main__':
    table_name_list = "StaffBankSecondment,StaffBankSecondment_His"
    src_engine = create_engine('mysql+pymysql://root@10.158.34.175:9030/StaffBank')
    tar_engine = create_engine('mysql+pymysql://root@10.158.34.175:9030/StaffBank_tmp')
    st = time.time()
    for tableName in table_name_list.split(","):
        run(src_engine, tar_engine, tableName)
    print('time : ', time.time() - st)
