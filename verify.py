import pandas as pd
import urllib
from sqlalchemy import text
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import json

startDate = '2020-07-01'
endDate = '2023-10-11'


def get_talent_link_org(sqlserver_engine):
    sql = f""" 
    SELECT 
    EmployeeID AS employeeID,
    JobID AS jobId, 
    StaffID AS  staffID
    FROM dbo.tblTalentLinkOrignal
    where 
    CreateByDate >= \'{startDate}\' AND CreateByDate < \'{endDate}\'
    """
    talent_link_result = pd.read_sql(text(sql), sqlserver_engine.connect())
    org_list = []
    for index, row in talent_link_result.iterrows():
        tmp_str = str(row['employeeID']) + str(row['jobId']) + str(row['staffID'][-6:])
        org_list.append(tmp_str)
    return org_list


def get_talent_link_tar(doris_engine):
    talent_link_result = pd.read_sql(
        text("select staff_id,job_id,employee_id from ods_advisory_talent_link group by staff_id, job_id, employee_id"),
        doris_engine.connect())
    tar_list = []
    for index, row in talent_link_result.iterrows():
        tmp_str = str(row['employee_id']) + str(row['job_id']) + str(row['staff_id'][-6:])
        tar_list.append(tmp_str)
    return tar_list


def write_to_json(file_name, data):
    json_str = json.dumps(data)
    # 将JSON字符串写入文件
    with open(f"./{file_name}", "w") as file:
        file.write(json_str)


if __name__ == '__main__':
    sqlserverEngine = create_engine("mssql+pymssql://TL_ADV_Reader:%s@CNSHADBSPWV001:1433/TalentLinkDBAdv" \
                                    % (urllib.parse.quote_plus('Ac1a7k0wG4bD')))
    tarDorisEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/advisory_engagement_lifecycle")
    org_datas = get_talent_link_org(sqlserverEngine)
    tar_datas = get_talent_link_tar(tarDorisEngine)
    tar_data_miss = []
    org_data_miss = []
    for value in org_datas:
        if value not in tar_datas:
            tar_data_miss.append(value)
    for value in tar_datas:
        if value not in org_datas:
            org_data_miss.append(value)
    write_to_json("miss_data_org.json", org_data_miss)
    write_to_json("miss_data_tar.json", tar_data_miss)
