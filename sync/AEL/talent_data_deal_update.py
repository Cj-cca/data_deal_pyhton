import pandas as pd
import pyodbc
import urllib
from sqlalchemy import text
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import json
from urllib.parse import quote_plus as urlquote

cnHolidayList = []
hkHolidayList = []

startDate = ''
endDate = ''

if startDate == '':
    startDate = datetime.now().date() - timedelta(days=8)
    endDate = datetime.now().date()

holidayStartDate = datetime.now().date() - timedelta(days=365 * 2)

fieldMapping = {'bookingID': 'booking_id', 'workerID': 'worker_id', 'officeCode': 'office_code', 'jobCode': 'job_code',
                'employeeID': 'employee_id', 'jobId': 'job_id', 'countryCode': 'country_code',
                'StaffName': 'staff_name', 'JobTitle': 'job_title', 'TermFlag': 'term_flag',
                'clientCode': 'client_code', 'staffID': 'staff_id', 'holidayFlag': 'holiday_flag',
                'workHours': 'work_hours', 'loading': 'loading', 'startDate': 'start_date', 'endDate': 'end_date',
                'resID': 'res_id', 'jobIdDesc': 'job_id_desc', 'dateRange': 'date_range',
                'createByDate': 'create_by_date'}

delay_data = {}


def get_holiday_info(engine):
    sql = f""" 
     SELECT DATE_FORMAT(WorkDate,'%Y-%m-%d') AS WorkDate,CountryCode
     FROM LMS.Calendar 
     WHERE WorkDate >= \'{holidayStartDate}\' AND DateStatus IN ('H','S')
     """
    result = pd.read_sql(text(sql), engine.connect())
    for index, row in result.iterrows():
        if 'CN' == row['CountryCode']:
            cnHolidayList.append(row['WorkDate'])
        if 'HK' == row['CountryCode']:
            hkHolidayList.append(row['WorkDate'])


def set_staff_info(row, staff_id_map):
    sf_id = row["staffID"]
    ct_code = row["countryCode"]
    staff_name = ""
    job_title = ""
    term_flag = ""
    term_date = ""
    if sf_id in staff_id_map:
        ct_code = staff_id_map[sf_id][0]
        term_date = staff_id_map[sf_id][1]
        staff_name = staff_id_map[sf_id][2]
        job_title = staff_id_map[sf_id][3]
        term_flag = staff_id_map[sf_id][4]

    if ("CN" + sf_id) in staff_id_map:
        sf_id = "CN" + sf_id
        if term_date == "" or datetime.strptime(term_date, "%Y-%m-%d") < datetime.strptime(
                staff_id_map.get(sf_id)[1], "%Y-%m-%d"):
            ct_code = staff_id_map[sf_id][0]
            term_date = staff_id_map[sf_id][1]
            staff_name = staff_id_map[sf_id][2]
            job_title = staff_id_map[sf_id][3]
            term_flag = staff_id_map[sf_id][4]

    if ("HK" + sf_id) in staff_id_map:
        sf_id = "HK" + sf_id
        if term_date == "" or datetime.strptime(term_date, "%Y-%m-%d") < datetime.strptime(
                staff_id_map.get(sf_id)[1], "%Y-%m-%d"):
            ct_code = staff_id_map[sf_id][0]
            term_date = staff_id_map[sf_id][1]
            staff_name = staff_id_map[sf_id][2]
            job_title = staff_id_map[sf_id][3]
            term_flag = staff_id_map[sf_id][4]

    row["staffID"] = sf_id
    row["countryCode"] = ct_code
    row["StaffName"] = staff_name
    row["JobTitle"] = job_title
    row["TermFlag"] = term_flag
    return term_date


def get_talent_link(sqlserver_engine, doris_engine):
    doris_connect = doris_engine.connect()
    sql = f""" 
    SELECT 
    BookingID AS bookingID,
    EmployeeID AS employeeID,
    CASE WHEN CHARINDEX('CHN-CN',EmployeeID)>0 THEN 'CN' ELSE 'HK' END as countryCode,
    StartDate AS startDate  ,
    StartDateTime AS startDateTime  ,
    EndDate AS endDate , 
    EndDateTime AS endDateTime , 
    ClientCode  AS clientCode,
    ClientName AS clientName, 
    JobCode AS jobCode , 
    CAST(LOADING AS decimal(6, 2)) AS loading,
    JobID AS jobId, 
    OfficeCode AS officeCode,
    WorkerID AS workerID,
    StaffID AS  staffID,
    JOB_ID_DESCR AS  jobIdDesc,
    RES_ID AS  resID,
    concat(StartDate,' - ',EndDate) AS dateRange,
    UpdateDate AS  updateDate,
    CreateByDate AS  createByDate
    FROM dbo.tblTalentLinkOrignal
    where 
    CreateByDate >= \'{startDate}\' AND CreateByDate < \'{endDate}\' AND GHOST='C'
    """
    talent_link_result = pd.read_sql(text(sql), sqlserver_engine.connect())
    result = []
    if talent_link_result.size > 0:
        sql = f"""select CountryCode,StaffID,TermDate,StaffName,JobTitle,TermFlag from StaffBank.StaffBank"""
        staff_bank_result = pd.read_sql(text(sql), doris_connect)
        staff_id_map = {}
        for index, row in staff_bank_result.iterrows():
            if row["TermDate"] is not None:
                staff_id_map[row["StaffID"]] = [row["CountryCode"], row["TermDate"].strftime("%Y-%m-%d").split("T")[0],
                                                row["StaffName"], row["JobTitle"], row["TermFlag"]]
            else:
                staff_id_map[row["StaffID"]] = [row["CountryCode"], '1999-01-01', row["StaffName"], row["JobTitle"],
                                                row["TermFlag"]]

        for index, row in talent_link_result.iterrows():
            update_date = row["updateDate"]
            create_by_date = row["createByDate"]
            delta_day = (create_by_date - update_date).days
            if delta_day > 1:
                if delta_day in delay_data:
                    delay_data[delta_day].append(row['staffID'])
                else:
                    delay_data[delta_day] = [row['staffID']]
            term_date = set_staff_info(row, staff_id_map)

            if term_date == "":
                # staffBank中没有这个staffID,则需要先去staffIDList里面查询staffID，然后在到staffBank里面查询
                worker_id = row["workerID"]
                sql = f"""select StaffID from StaffBank.StaffIDList where  Worker_ID = \'{worker_id}\' order by UpdateTime desc limit 1"""
                staff_id_list_result = pd.read_sql(text(sql), doris_connect)
                if staff_id_list_result.size > 0:
                    sf_id = staff_id_list_result.iloc[0]["StaffID"]
                    row["staffID"] = sf_id
                    term_date = set_staff_info(row, staff_id_map)
                    if term_date == "":
                        print(f"StaffBank.StaffBank没有该staff_id: {sf_id}")
                else:
                    print(f"StaffBank.StaffIDList没有该worker_id:{worker_id}")

            loading = row["loading"]
            country_code = row["countryCode"]

            item = {"bookingID": row["bookingID"], "workerID": row["workerID"], "officeCode": row["officeCode"],
                    "jobCode": row["jobCode"], "employeeID": row["employeeID"], "jobId": row["jobId"],
                    "countryCode": row["countryCode"], "clientCode": row["clientCode"], "staffID": row["staffID"],
                    "StaffName": row["StaffName"], "JobTitle": row["JobTitle"], "TermFlag": row["TermFlag"],
                    "resID": row["resID"], "jobIdDesc": row["jobIdDesc"], "dateRange": row["dateRange"],
                    "createByDate": row["createByDate"]}

            start_date = row["startDate"]
            end_date = row["endDate"]
            # 将开始时间和结束时间转换为浮点数
            start_date_time = convert_hour_to_float(row["startDateTime"])
            end_date_time = convert_hour_to_float(row["endDateTime"])

            date_diff = abs(start_date - end_date)
            if date_diff.days > 1:
                # print("日期差大于一天")
                # print("先处理开始那天和结束那天")
                start_date_str = start_date.strftime("%Y-%m-%d")
                work_hour = calculate_hour(start_date_time, 17.5)
                add_item_for_result(start_date_str, item, country_code, loading, result, work_hour)
                start_date_str = end_date.strftime("%Y-%m-%d")
                work_hour = calculate_hour(9, end_date_time)
                add_item_for_result(start_date_str, item, country_code, loading, result, work_hour)
                # print("处理中间的时间日期")
                start_date_new = start_date + timedelta(days=1)
                end_date_new = end_date - timedelta(days=1)
                start_date_str = start_date_new.strftime("%Y-%m-%d")
                add_items_for_result(start_date_new, end_date_new, start_date_str, item, country_code, loading, result)

            elif date_diff.days == 1:
                # print("日期差等于一天")
                start_date_str = start_date.strftime("%Y-%m-%d")
                work_hour = calculate_hour(start_date_time, 17.5)
                add_item_for_result(start_date_str, item, country_code, loading, result, work_hour)
                start_date_str = end_date.strftime("%Y-%m-%d")
                work_hour = calculate_hour(9, end_date_time)
                add_item_for_result(start_date_str, item, country_code, loading, result, work_hour)
            else:
                # print("都是当天")
                start_date_str = end_date.strftime("%Y-%m-%d")
                work_hour = calculate_hour(start_date_time, end_date_time)
                add_item_for_result(start_date_str, item, country_code, loading, result, work_hour)
        doris_connect.close()
    return result


# 9：00 - 12：00 工作时间
# 12:00 - 12:30  休息时间
# 12：30 - 17:30 工作时间
def calculate_hour(start_hour, end_hour):
    if start_hour < 9:
        start_hour = 9
        if end_hour <= 12:
            hour = end_hour - start_hour
        elif 12 < end_hour < 12.5:
            hour = 12 - start_hour
        else:
            hour = end_hour - start_hour - 0.5
    elif 9 <= start_hour <= 12:
        if end_hour <= 12:
            hour = end_hour - start_hour
        elif 12 < end_hour < 12.5:
            hour = 12 - start_hour
        else:
            hour = end_hour - start_hour - 0.5
    elif 12 < start_hour <= 12.5:
        start_hour = 12.5
        hour = end_hour - start_hour
    else:
        hour = end_hour - start_hour
    return hour


def convert_hour_to_float(dt):
    # 获取小时和分钟
    hour = dt.hour
    minute = dt.minute

    # 将分钟转换为小时的小数部分
    return hour + minute / 60


def add_items_for_result(start_date_tmp, end_date_tmp, start_date_str, item, country_code, loading, result):
    while start_date_tmp <= end_date_tmp:
        add_item_for_result(start_date_str, item, country_code, loading, result, 8)
        start_date_tmp += timedelta(days=1)
        start_date_str = start_date_tmp.strftime("%Y-%m-%d")


def add_item_for_result(start_date_str, item, country_code, loading, result, work_hour):
    if "CN" == country_code and start_date_str in cnHolidayList:
        item["holidayFlag"] = 0
        item["workHours"] = 0.0
        item["loading"] = 0
    elif "HK" == country_code and start_date_str in hkHolidayList:
        item["holidayFlag"] = 0
        item["workHours"] = 0.0
        item["loading"] = 0
    else:
        item["holidayFlag"] = 1
        item["workHours"] = loading * 0.01 * work_hour
        item["loading"] = loading
    item["startDate"] = start_date_str
    item["endDate"] = start_date_str
    tmp = item.copy()
    result.append(tmp)


if __name__ == '__main__':
    sqlserverEngine = create_engine("mssql+pymssql://TL_ADV_Reader:%s@CNSHADBSPWV001:1433/TalentLinkDBAdv" \
                                    % (urllib.parse.quote_plus('Ac1a7k0wG4bD')))
    dorisEngine = create_engine('mysql+pymysql://root@10.158.34.175:9030/StaffBank')
    tarDorisEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/advisory_engagement_lifecycle")
    get_holiday_info(dorisEngine)
    result_data = get_talent_link(sqlserverEngine, dorisEngine)
    tableName = "ods_advisory_talent_link_update_field_and_key_tmp"
    df = pd.DataFrame(result_data)
    df.rename(columns=fieldMapping, inplace=True)
    result_count = df.to_sql(tableName, tarDorisEngine, if_exists='append', index=False)
    print(f"数据写入成功，数据条数：{len(result_data)}。写入数据条数：{result_count}")
    if len(delay_data) > 0:
        print("delta date exceed 2 day: ")
        for key in delay_data:
            print(f"{key}: {delay_data[key]}")
