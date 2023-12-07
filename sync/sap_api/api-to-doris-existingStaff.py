# !/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import time
import json
import zlib
import pandas
import requests
import base64
from sqlalchemy import create_engine
from sqlalchemy import text

# data sync complete, max cost time is 60.72, avg cost time is 33.355185185185185
notNullField = 'worker_ID'
FieldsDate = ['Employment_Start_Date_1', 'LeaveStartDate', 'Employment_Start_Date_0', 'Worker_Contract_End_Date',
              'Effective_Date_1', 'Effective_Date_0', 'Employee_Contract_End_Date', 'BirthDate', 'JoinDate',
              'Issued_Date', 'Company_Service_Date', 'Expected_Assignment_End_Date_1', 'Expected_Assignment_End_Date_0',
              'Compensation_Effective_Date', 'LeaveEndDate', 'End_International_Employment_Date_1',
              'Employee_Contract_Effective_Date', 'End_International_Employment_Date_0', 'TermDate']
fieldMapping = {'location_0': 'Location_0', 'employment_Start_Date_1': 'Employment_Start_Date_1',
                'department_Name_2': 'Department_Name_2',
                'cost_Center_Hierarchy_Code': 'Cost_Center_Hierarchy_Code', 'worker_ID': 'Worker_ID',
                'officeCity_1': 'OfficeCity_1', 'GUID': 'GUID', 'HKG_Staff_ID': 'HKG_Staff_ID',
                'public_Email_Secondary': 'Public_Email_Secondary',
                'coach_Reference_ID_2': 'Coach_Reference_ID_2', 'leaveStartDate': 'LeaveStartDate',
                'exclude_from_Headcount_0': 'Exclude_from_Headcount_0', 'CHN_Staff_ID': 'CHN_Staff_ID',
                'jobGrade': 'JobGrade', 'global_Line_of_Service_Name': 'Global_Line_of_Service_Name',
                'cost_Center_Name_2': 'Cost_Center_Name_2',
                'cost_Center_Reference_ID': 'Cost_Center_Reference_ID',
                'education_Country': 'Education_Country', 'home_Country': 'Home_Country',
                'name_Suffix_Data': 'Name_Suffix_Data', 'organization_Role_ID': 'Organization_Role_ID',
                'fullName': 'FullName', 'employment_Start_Date_0': 'Employment_Start_Date_0',
                'contingent_Worker_ID': 'Contingent_Worker_ID', 'coach_Descriptor': 'Coach_Descriptor',
                'host_Country': 'Host_Country', 'countryOffice_1': 'CountryOffice_1',
                'job_Profile_Reference_ID_0': 'Job_Profile_Reference_ID_0',
                'qualificationType': 'QualificationType',
                'exclude_from_Headcount_1': 'Exclude_from_Headcount_1',
                'mobile_PhoneNo_Secondary': 'Mobile_PhoneNo_Secondary', 'company_Name': 'Company_Name',
                'nameInEng': 'NameInEng',
                'cost_Center_Hierarchy_Reference_ID': 'Cost_Center_Hierarchy_Reference_ID',
                'global_Network_Reference_ID': 'Global_Network_Reference_ID',
                'company_Reference_ID_2': 'Company_Reference_ID_2', 'degree': 'Degree',
                'pwC_Location_Hierarchy_Name_2': 'PwC_Location_Hierarchy_Name_2',
                'worker_Contract_End_Date': 'Worker_Contract_End_Date', 'nationality': 'Nationality',
                'nativeName': 'NativeName',
                'global_Line_of_Service_Reference_ID_2': 'Global_Line_of_Service_Reference_ID_2',
                'lastName': 'LastName', 'HKG_iPower_ID': 'HKG_iPower_ID', 'CHN_iPower_ID': 'CHN_iPower_ID',
                'countryOffice_0': 'CountryOffice_0', 'last_NameCN': 'Last_NameCN',
                'management_Level_ID_0': 'Management_Level_ID_0',
                'global_Network_Name_2': 'Global_Network_Name_2',
                'start_International_Assignment_Reason_0': 'Start_International_Assignment_Reason_0',
                'start_International_Assignment_Reason_1': 'Start_International_Assignment_Reason_1',
                'private_Email_Primary': 'Private_Email_Primary', 'organization_Role': 'Organization_Role',
                'assignment_Type_0': 'Assignment_Type_0', 'effective_Date_1': 'Effective_Date_1',
                'workingTerritory_0': 'WorkingTerritory_0', 'effective_Date_0': 'Effective_Date_0',
                'workingTerritory_1': 'WorkingTerritory_1', 'CHN_HRID': 'CHN_HRID',
                'employee_Contract_End_Date': 'Employee_Contract_End_Date',
                'employeeCategory_0': 'EmployeeCategory_0', 'assignment_Type_1': 'Assignment_Type_1',
                'department_Reference_ID': 'Department_Reference_ID', 'cost_Center_Code': 'Cost_Center_Code',
                'public_Email_Primary': 'Public_Email_Primary', 'status': 'Status', 'birthDate': 'BirthDate',
                'business_Title_0': 'Business_Title_0', 'company_Reference_ID': 'Company_Reference_ID',
                'landline_PhoneNo_Primary': 'Landline_PhoneNo_Primary', 'majority': 'Majority',
                'joinDate': 'JoinDate', 'passport_ID': 'Passport_ID',
                'cost_Center_Hierarchy_Name': 'Cost_Center_Hierarchy_Name',
                'global_Network_Reference_ID_2': 'Global_Network_Reference_ID_2',
                'cost_Center_Hierarchy_Reference_ID_2': 'Cost_Center_Hierarchy_Reference_ID_2',
                'employeeCategory_1': 'EmployeeCategory_1', 'management_Level_1': 'Management_Level_1',
                'issued_Date': 'Issued_Date', 'first_NameCN': 'First_NameCN',
                'management_Level_0': 'Management_Level_0', 'business_Title_1': 'Business_Title_1',
                'employee_Location_Name_0': 'Employee_Location_Name_0',
                'pwC_Location_Hierarchy_Reference_ID': 'PwC_Location_Hierarchy_Reference_ID',
                'on_Leave_Status': 'On_Leave_Status', 'postal_Code_0': 'Postal_Code_0',
                'job_Family_Reference_0': 'Job_Family_Reference_0', 'HKG_HRID': 'HKG_HRID',
                'company_Service_Date': 'Company_Service_Date', 'global_Network_Name': 'Global_Network_Name',
                'employee_Location_Name_1': 'Employee_Location_Name_1',
                'citizenship_Status': 'Citizenship_Status', 'first_Name': 'First_Name',
                'expected_Assignment_End_Date_1': 'Expected_Assignment_End_Date_1', 'party_ID': 'Party_ID',
                'job_Category_0': 'Job_Category_0',
                'pwC_Location_Hierarchy_Reference_ID_2': 'PwC_Location_Hierarchy_Reference_ID_2',
                'job_Family_Reference_1': 'Job_Family_Reference_1',
                'expected_Assignment_End_Date_0': 'Expected_Assignment_End_Date_0', 'PPI': 'PPI',
                'cost_Center_Hierarchy_Name_2': 'Cost_Center_Hierarchy_Name_2',
                'compensation_Effective_Date': 'Compensation_Effective_Date',
                'cost_Center_Hierarchy_Code_2': 'Cost_Center_Hierarchy_Code_2',
                'cost_Center_Name': 'Cost_Center_Name',
                'pwC_Location_Hierarchy_Name': 'PwC_Location_Hierarchy_Name',
                'job_Category_1': 'Job_Category_1', 'postal_Code_1': 'Postal_Code_1',
                'end_Employment_Reason_Reference_0': 'End_Employment_Reason_Reference_0',
                'management_Level_ID_1': 'Management_Level_ID_1',
                'national_Country_Reference': 'National_Country_Reference',
                'landline_PhoneNo_Secondary': 'Landline_PhoneNo_Secondary',
                'officeAddressDesc_1': 'OfficeAddressDesc_1', 'coach_Descriptor_2': 'Coach_Descriptor_2',
                'private_Email_Secondary': 'Private_Email_Secondary', 'national_ID': 'National_ID',
                'ext': 'Ext', 'sex': 'Sex', 'leaveEndDate': 'LeaveEndDate', 'officeCity_0': 'OfficeCity_0',
                'host_Country_2': 'Host_Country_2', 'schoolID': 'SchoolID', 'jobGradeDesc': 'JobGradeDesc',
                'home_Country_2': 'Home_Country_2',
                'cost_Center_Reference_ID_2': 'Cost_Center_Reference_ID_2',
                'department_Name': 'Department_Name', 'pay_Group_0': 'Pay_Group_0',
                'end_International_Employment_Date_1': 'End_International_Employment_Date_1',
                'department_Reference_ID_2': 'Department_Reference_ID_2',
                'employee_Contract_Effective_Date': 'Employee_Contract_Effective_Date',
                'end_International_Employment_Date_0': 'End_International_Employment_Date_0',
                'has_International_Assignment': 'Has_International_Assignment',
                'cost_Center_Code_2': 'Cost_Center_Code_2', 'officeAddressDesc_0': 'OfficeAddressDesc_0',
                'coach_Reference_ID': 'Coach_Reference_ID', 'termDate': 'TermDate',
                'hire_Reason_Reference': 'Hire_Reason_Reference', 'schoolUniversity': 'SchoolUniversity',
                'end_Employment_Reason_Reference_1': 'End_Employment_Reason_Reference_1',
                'pay_Group_1': 'Pay_Group_1', 'job_Profile_Reference_ID_1': 'Job_Profile_Reference_ID_1',
                'careerCoach': 'CareerCoach', 'company_Name_2': 'Company_Name_2',
                'global_Line_of_Service_Name_2': 'Global_Line_of_Service_Name_2', 'gradeCode': 'GradeCode',
                'mobile_PhoneNo_Primary': 'Mobile_PhoneNo_Primary', 'location_1': 'Location_1',
                'global_Line_of_Service_Reference_ID': 'Global_Line_of_Service_Reference_ID'}

resultColumn_ods = ['page_index',
                    'create_time',
                    'response_data',
                    'response_object_count',
                    'query_url'
                    ]

resultColumn_ods_detail = ['batch_id',
                           'create_time',
                           'current_page_num',
                           'response_data',
                           'response_object_num',
                           'query_url',
                           'comment'
                           ]

tarExistingStaffTableNameDwd = "ExistingStaff_dwd"
tarExistingStaffTableNameOds = "ExistingStaff_ods"
tarExistingStaffTableNameOdsDetail = "ExistingStaff_ods_detail"

tarEngine = create_engine('mysql+pymysql://root@10.158.16.244:9030/WorkDayStage')
batchID = 0
avgCostTime = 0
maxCostTime = 0
createTime = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def syncApiData(user_name, password, query_url, page_index=1, offset=0):
    global avgCostTime, maxCostTime
    credentials = f"{user_name}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    auth = f"Basic {encoded_credentials}"
    headers = {
        "Authorization": auth,
        "skip": str(offset),
        "top": '1000'
    }
    proxies = requests.utils.getproxies()
    if proxies and 'https' in proxies:
        proxies['https'] = proxies['http']
    print(f"======start sync data from page {page_index}======")
    start = time.time()
    response = requests.get(query_url, headers=headers, proxies=proxies, verify=False).json()
    cost_time = round(time.time() - start, 2)
    avgCostTime += cost_time
    if maxCostTime < cost_time:
        maxCostTime = cost_time
    print(f"page {page_index} data select complete, cost {round(time.time() - start, 2)}s")
    deal_ods(response, query_url, page_index)
    deal_ods_detail(response, query_url, page_index)
    deal_dwd(response)
    print(f"======page {page_index} data sync complete, cost {round(time.time() - start, 2)}s======")
    if len(response['fullWorkerList']) != 0:
        page_index += 1
        offset += 1000
        syncApiData(user_name, password, query_url, page_index=page_index, offset=offset)
    else:
        avgCostTime /= page_index


def deal_ods_detail(response, query_url, page_index=1):
    global batchID
    dataNumber = 0
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods_detail)
    for value in response["fullWorkerList"]:
        batchID += 1
        dataNumber += 1
        comment = "" if value[f"{notNullField}"] is not None else "Exception data， externalCode is None"
        tmp_dataframe.loc[len(tmp_dataframe.index)] = [batchID, createTime, page_index,
                                                       json.dumps(value), dataNumber, query_url, comment]
    insert_count = tmp_dataframe.to_sql(tarExistingStaffTableNameOdsDetail, tarEngine, if_exists='append', index=False)
    print(f"{tarExistingStaffTableNameOdsDetail}数据插入成功，受影响行数：", insert_count)


def deal_ods(response, query_url, page_index):
    tmp_dataframe = pandas.DataFrame(columns=resultColumn_ods)
    originalParentCount = len(response["fullWorkerList"])
    str_response = json.dumps(response).encode('utf-8')
    compressed_response = zlib.compress(str_response)
    compressed_response_str = base64.b64encode(compressed_response).decode('utf-8')
    tmp_dataframe.loc[len(tmp_dataframe.index)] = [page_index, createTime, compressed_response_str,
                                                   originalParentCount, query_url]
    insert_count = tmp_dataframe.to_sql(tarExistingStaffTableNameOds, tarEngine, if_exists='append', index=False)
    print(f"{tarExistingStaffTableNameOds}数据插入成功，受影响行数：", insert_count)


def deal_date_list(date_list):
    dates = []
    for date in date_list.split(','):
        dt = format_date(date).strip()
        if dt != '':
            dates.append(dt)
    return ','.join(dates)


# 将date:2021-12-20T00:00:00.000格式化为：1/1/0001 12:00:00 AM
def format_date(date):
    result = ''
    if date == '' or date == 'null':
        return result
    if len(date) > 18:
        date_obj = datetime.datetime.fromisoformat(date)
        result = date_obj.strftime('%m/%d/%Y %I:%M:%S %p')
    elif len(date) == 4:
        date = f"{date}-01-01T12:00:00.000"
        date_obj = datetime.datetime.fromisoformat(date)
        result = date_obj.strftime('%m/%d/%Y %I:%M:%S %p')
    else:
        print("Exception: unknown date format")
    return result


def format_fields_date(dataframe, index):
    for field_date in FieldsDate:
        date = dataframe.loc[index, field_date]
        dataframe.loc[index, field_date] = deal_date_list(date)


def deal_dwd(response):
    data_frame = pandas.json_normalize(response, record_path=["fullWorkerList"])
    data_frame.rename(columns=fieldMapping, inplace=True)
    result_dataframe = data_frame.fillna('')
    for i in range(0, len(result_dataframe.index)):
        format_fields_date(result_dataframe, i)
    insert_count = result_dataframe.to_sql(tarExistingStaffTableNameDwd, tarEngine, if_exists='append', index=False)
    print(f"{tarExistingStaffTableNameDwd}数据插入成功，总数据条数: {len(result_dataframe)}，插入行数：{insert_count}")


def truncateTable(engine, table_name):
    tarConn = engine.connect()
    tarConn.execute(text(f"truncate table {table_name}"))
    tarConn.close()
    print("table truncate is complete")


if __name__ == '__main__':
    user = "sb-a8531244-374b-414c-8944-0dbdf941c2e5!b1813|it-rt-pwc-dev!b39"
    pwd = "f4aee3e5-9539-4568-be98-404a5c6ca253$yxW2FNy_fKA8a1Fjn44SM3zjSt4VGvIbzu9tQnHfWdg="
    existing_staff_url = "https://pwc-dev.it-cpi010-rt.cpi.cn40.apps.platform.sapcloud.cn/http/vprofile/existingstaff"
    truncateTable(tarEngine, tarExistingStaffTableNameDwd)
    syncApiData(user, pwd, existing_staff_url)
    print(f"data sync complete, max cost time is {maxCostTime}, avg cost time is {avgCostTime}")
