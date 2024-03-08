import pandas as pd
import time
import urllib
from sqlalchemy import text
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import json
import queue
import threading
import concurrent.futures
from urllib.parse import quote_plus as urlquote

cnHolidayList = []
hkHolidayList = []

startDate = '2018-08-16'
endDate = '2024-02-01'
# startDate = '2018-08-16'
# endDate = '2023-10-11'

if startDate == '':
    startDate = datetime.now().date() - timedelta(days=8)
    endDate = datetime.now().date()

fieldMapping = {'bookingID': 'booking_id', 'workerID': 'worker_id', 'officeCode': 'office_code', 'jobCode': 'job_code',
                'employeeID': 'employee_id', 'jobId': 'job_id', 'countryCode': 'country_code',
                'StaffName': 'staff_name', 'JobTitle': 'job_title', 'TermFlag': 'term_flag',
                'clientCode': 'client_code', 'staffID': 'staff_id', 'holidayFlag': 'holiday_flag',
                'workHours': 'work_hours', 'loading': 'loading', 'startDate': 'start_date', 'endDate': 'end_date',
                'resID': 'res_id', 'jobIdDesc': 'job_id_desc', 'dateRange': 'date_range',
                'createByDate': 'create_by_date'}

delay_data = {}
staff_id_map = {}


def get_holiday_info(engine):
    sql = f""" 
     SELECT DATE_FORMAT(WorkDate,'%Y-%m-%d') AS WorkDate,CountryCode
     FROM LMS.Calendar 
     WHERE DateStatus IN ('H','S')
     """
    result = pd.read_sql(text(sql), engine.connect())
    for index, row in result.iterrows():
        if 'CN' == row['CountryCode']:
            cnHolidayList.append(row['WorkDate'])
        if 'HK' == row['CountryCode']:
            hkHolidayList.append(row['WorkDate'])


def get_staff_info(doris_engine):
    doris_connect = doris_engine.connect()
    sql = f"""select CountryCode,StaffID,TermDate,StaffName,JobTitle,TermFlag from StaffBank.StaffBank"""
    staff_bank_result = pd.read_sql(text(sql), doris_connect)
    for index, row in staff_bank_result.iterrows():
        if row["TermDate"] is not None:
            staff_id_map[row["StaffID"]] = [row["CountryCode"], row["TermDate"].strftime("%Y-%m-%d").split("T")[0],
                                            row["StaffName"], row["JobTitle"], row["TermFlag"]]
        else:
            staff_id_map[row["StaffID"]] = [row["CountryCode"], '1999-01-01', row["StaffName"], row["JobTitle"],
                                            row["TermFlag"]]


def set_staff_info(row):
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


def get_talent_link(start, end, sqlserver_engine):
    sql = f""" 
    SELECT 
    BookingID AS bookingID,
    EmployeeID AS employeeID,
    CASE WHEN CHARINDEX('CHN-CN',EmployeeID)>0 THEN 'CN' ELSE 'HK' END as countryCode,
    StartDate AS startDate  ,
    EndDate AS endDate , 
    ClientCode  AS clientCode,
    ClientName AS clientName, 
    JobCode AS jobCode , 
    CAST(LOADING AS decimal(6, 2)) AS loading,
    CAST(LOADING AS decimal(6, 2))*0.08 AS workHours ,
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
    CreateByDate >= \'{start}\' AND CreateByDate < \'{end}\' AND GHOST='C'
    """
    sqlserver_conn = sqlserver_engine.connect()
    talent_link_result = pd.read_sql(text(sql), sqlserver_conn)
    sqlserver_conn.close()
    return talent_link_result


def truncateTable(engine, table_name):
    tarConn = engine.connect()
    tarConn.execute(text(f"truncate table {table_name}"))
    tarConn.close()
    print("table truncate is complete")


def task_producer(tq):
    # 在这里生成任务并放入队列
    start_date = datetime.strptime(startDate, "%Y-%m-%d")
    end_date = datetime.strptime(endDate, "%Y-%m-%d")
    sqlserver_engine = create_engine("mssql+pymssql://TL_ADV_Reader:%s@CNSHADBSPWV001:1433/TalentLinkDBAdv" \
                                     % (urllib.parse.quote_plus('Ac1a7k0wG4bD')))
    while start_date < end_date:
        tmp_end_date = start_date + timedelta(days=30)
        print(f"{start_date}<->{tmp_end_date}: 获取当前时间段数据开始！！！")
        try:
            result_data = get_talent_link(start_date, tmp_end_date, sqlserver_engine)
            tq.put(result_data)
            print(f"{start_date}<->{tmp_end_date}: 获取当前时间段数据完成！！！，数据长度：{len(result_data)}")
            start_date = tmp_end_date
        except Exception as e:
            print(f"获取原始数据发生异常：{e}")


def write_data(talent_link_result):
    print("开始处理原始数据")
    old_doris_engine = create_engine('mysql+pymysql://root@10.158.34.175:9030/StaffBank')
    doris_connect = old_doris_engine.connect()
    result = []
    if talent_link_result.size > 0:
        for index, row in talent_link_result.iterrows():
            update_date = row["updateDate"]
            create_by_date = row["createByDate"]
            delta_day = (create_by_date - update_date).days
            if delta_day > 1:
                if delta_day in delay_data:
                    delay_data[delta_day].append(row['staffID'])
                else:
                    delay_data[delta_day] = [row['staffID']]
            term_date = set_staff_info(row)

            if term_date == "":
                # staffBank中没有这个staffID,则需要先去staffIDList里面查询staffID，然后在到staffBank里面查询
                worker_id = row["workerID"]
                sql = f"""select StaffID from StaffBank.StaffIDList where  Worker_ID = \'{worker_id}\' order by UpdateTime desc limit 1"""
                staff_id_list_result = pd.read_sql(text(sql), doris_connect)
                if staff_id_list_result.size > 0:
                    sf_id = staff_id_list_result.iloc[0]["StaffID"]
                    row["staffID"] = sf_id
                    term_date = set_staff_info(row)
                    if term_date == "":
                        print(f"StaffBank.StaffBank没有该staff_id: {sf_id}")
                else:
                    print(f"StaffBank.StaffIDList没有该worker_id:{worker_id}")

            work_hours = row["workHours"]
            loading = row["loading"]
            country_code = row["countryCode"]

            item = {"bookingID": row["bookingID"], "workerID": row["workerID"], "officeCode": row["officeCode"],
                    "jobCode": row["jobCode"], "employeeID": row["employeeID"], "jobId": row["jobId"],
                    "countryCode": row["countryCode"], "clientCode": row["clientCode"], "staffID": row["staffID"],
                    "StaffName": row["StaffName"], "JobTitle": row["JobTitle"], "TermFlag": row["TermFlag"],
                    "resID": row["resID"], "jobIdDesc": row["jobIdDesc"], "dateRange": row["dateRange"],
                    "createByDate": row["createByDate"]}

            start_date_str = row["startDate"].strftime("%Y-%m-%d")
            start_date_tmp = row["startDate"]
            end_date_tmp = row["endDate"]

            while start_date_tmp <= end_date_tmp:
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
                    item["workHours"] = work_hours
                    item["loading"] = loading
                item["startDate"] = start_date_str
                item["endDate"] = start_date_str
                tmp = item.copy()
                result.append(tmp)
                start_date_tmp += timedelta(days=1)
                start_date_str = start_date_tmp.strftime("%Y-%m-%d")
        doris_connect.close()
    tar_doris_engine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/advisory_engagement_lifecycle")
    tableName = "ods_advisory_talent_link_update_field_and_key_tmp"
    df = pd.DataFrame(result)
    df.rename(columns=fieldMapping, inplace=True)
    try:
        result_count = df.to_sql(tableName, tar_doris_engine, if_exists='append', index=False)
        print(f"数据写入成功，数据条数：{len(result)}。写入数据条数：{result_count}")
    except Exception as e:
        print(e)


if __name__ == '__main__':
    # 问题解决，可以投入使用
    start_run = time.time()
    oldDorisEngine = create_engine('mysql+pymysql://root@10.158.34.175:9030/StaffBank')
    get_holiday_info(oldDorisEngine)
    get_staff_info(oldDorisEngine)
    max_queue_size = 20
    taskQueue = queue.Queue(max_queue_size)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as t:
        # 启动任务生产者线程
        producer_thread = threading.Thread(target=task_producer, args=(taskQueue,))
        producer_thread.start()
        time.sleep(10)
        while True:
            try:
                resultData = taskQueue.get(timeout=120)  # 设置超时以便在队列为空时跳出循环
                t.submit(write_data, resultData)
            except queue.Empty:
                print("队列没有可获取的数据")
                break
    print("等待子线程任务执行完成！")
    # 主线程等待线程池中的任务执行完成
    t.shutdown(wait=True)
    # 主线程等待生产者线程执行完
    producer_thread.join()
    print(f"任务执行完成，总用时：{round(time.time() - start_run, 2)}s")
