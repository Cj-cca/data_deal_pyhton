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
from urllib.parse import quote_plus as urlquote

# data sync complete, max cost time is 60.72, avg cost time is 33.355185185185185
notNullField = 'worker_ID'
FieldsDate = ['employment_start_date_1', 'leave_start_date', 'employment_start_date_0', 'worker_contract_end_date',
              'effective_date_1', 'effective_date_0', 'employee_contract_end_date', 'birth_date', 'join_date',
              'issued_date', 'company_service_date', 'expected_assignment_end_date_1', 'expected_assignment_end_date_0',
              'compensation_effective_date', 'leave_end_date', 'end_international_employment_date_1',
              'employee_contract_effective_date', 'end_international_employment_date_0', 'term_date']

fieldMapping = {'location_0': 'location_0', 'employment_Start_Date_1': 'employment_start_date_1',
                'department_Name_2': 'department_name_2', 'cost_Center_Hierarchy_Code': 'cost_center_hierarchy_code',
                'worker_ID': 'worker_id', 'officeCity_1': 'office_city_1', 'GUID': 'guid',
                'HKG_Staff_ID': 'hkg_staff_id', 'public_Email_Secondary': 'public_email_secondary',
                'coach_Reference_ID_2': 'coach_reference_id_2', 'leaveStartDate': 'leave_start_date',
                'exclude_from_Headcount_0': 'exclude_from_headcount_0', 'CHN_Staff_ID': 'chn_staff_id',
                'jobGrade': 'job_grade', 'global_Line_of_Service_Name': 'global_line_of_service_name',
                'cost_Center_Name_2': 'cost_center_name_2', 'cost_Center_Reference_ID': 'cost_center_reference_id',
                'education_Country': 'education_country', 'home_Country': 'home_country',
                'name_Suffix_Data': 'name_suffix_data', 'organization_Role_ID': 'organization_role_id',
                'fullName': 'full_name', 'employment_Start_Date_0': 'employment_start_date_0',
                'contingent_Worker_ID': 'contingent_worker_id', 'coach_Descriptor': 'coach_descriptor',
                'host_Country': 'host_country', 'countryOffice_1': 'country_office_1',
                'job_Profile_Reference_ID_0': 'job_profile_reference_id_0', 'qualificationType': 'qualification_type',
                'exclude_from_Headcount_1': 'exclude_from_headcount_1',
                'mobile_PhoneNo_Secondary': 'mobile_phone_no_secondary', 'company_Name': 'company_name',
                'nameInEng': 'name_in_eng', 'cost_Center_Hierarchy_Reference_ID': 'cost_center_hierarchy_reference_id',
                'global_Network_Reference_ID': 'global_network_reference_id',
                'company_Reference_ID_2': 'company_reference_id_2', 'degree': 'degree',
                'pwC_Location_Hierarchy_Name_2': 'pwc_location_hierarchy_name_2',
                'worker_Contract_End_Date': 'worker_contract_end_date', 'nationality': 'nationality',
                'nativeName': 'native_name',
                'global_Line_of_Service_Reference_ID_2': 'global_line_of_service_reference_id_2',
                'lastName': 'last_name', 'HKG_iPower_ID': 'hkg_ipower_id', 'CHN_iPower_ID': 'chn_ipower_id',
                'countryOffice_0': 'country_office_0', 'last_NameCN': 'last_name_cn',
                'management_Level_ID_0': 'management_level_id_0', 'global_Network_Name_2': 'global_network_name_2',
                'start_International_Assignment_Reason_0': 'start_international_assignment_reason_0',
                'start_International_Assignment_Reason_1': 'start_international_assignment_reason_1',
                'private_Email_Primary': 'private_email_primary', 'organization_Role': 'organization_role',
                'assignment_Type_0': 'assignment_type_0', 'effective_Date_1': 'effective_date_1',
                'workingTerritory_0': 'working_territory_0', 'effective_Date_0': 'effective_date_0',
                'workingTerritory_1': 'working_territory_1', 'CHN_HRID': 'chn_hr_id',
                'employee_Contract_End_Date': 'employee_contract_end_date', 'employeeCategory_0': 'employee_category_0',
                'assignment_Type_1': 'assignment_type_1', 'department_Reference_ID': 'department_reference_id',
                'cost_Center_Code': 'cost_center_code', 'public_Email_Primary': 'public_email_primary',
                'status': 'status', 'birthDate': 'birth_date', 'business_Title_0': 'business_title_0',
                'company_Reference_ID': 'company_reference_id', 'landline_PhoneNo_Primary': 'landline_phone_no_primary',
                'majority': 'majority', 'joinDate': 'join_date', 'passport_ID': 'passport_id',
                'cost_Center_Hierarchy_Name': 'cost_center_hierarchy_name',
                'global_Network_Reference_ID_2': 'global_network_reference_id_2',
                'cost_Center_Hierarchy_Reference_ID_2': 'cost_center_hierarchy_reference_id_2',
                'employeeCategory_1': 'employee_category_1', 'management_Level_1': 'management_level_1',
                'issued_Date': 'issued_date', 'first_NameCN': 'first_name_cn',
                'management_Level_0': 'management_level_0', 'business_Title_1': 'business_title_1',
                'employee_Location_Name_0': 'employee_location_name_0',
                'pwC_Location_Hierarchy_Reference_ID': 'pwc_location_hierarchy_reference_id',
                'on_Leave_Status': 'on_leave_status', 'postal_Code_0': 'postal_code_0',
                'job_Family_Reference_0': 'job_family_reference_0', 'HKG_HRID': 'hkg_hr_id',
                'company_Service_Date': 'company_service_date', 'global_Network_Name': 'global_network_name',
                'employee_Location_Name_1': 'employee_location_name_1', 'citizenship_Status': 'citizenship_status',
                'first_Name': 'first_name', 'expected_Assignment_End_Date_1': 'expected_assignment_end_date_1',
                'party_ID': 'party_id', 'job_Category_0': 'job_category_0',
                'pwC_Location_Hierarchy_Reference_ID_2': 'pwc_location_hierarchy_reference_id_2',
                'job_Family_Reference_1': 'job_family_reference_1',
                'expected_Assignment_End_Date_0': 'expected_assignment_end_date_0', 'PPI': 'ppi',
                'cost_Center_Hierarchy_Name_2': 'cost_center_hierarchy_name_2',
                'compensation_Effective_Date': 'compensation_effective_date',
                'cost_Center_Hierarchy_Code_2': 'cost_center_hierarchy_code_2', 'cost_Center_Name': 'cost_center_name',
                'pwC_Location_Hierarchy_Name': 'pwc_location_hierarchy_name', 'job_Category_1': 'job_category_1',
                'postal_Code_1': 'postal_code_1',
                'end_Employment_Reason_Reference_0': 'end_employment_reason_reference_0',
                'management_Level_ID_1': 'management_level_id_1',
                'national_Country_Reference': 'national_country_reference',
                'landline_PhoneNo_Secondary': 'landline_phone_no_secondary',
                'officeAddressDesc_1': 'office_address_desc_1', 'coach_Descriptor_2': 'coach_descriptor_2',
                'private_Email_Secondary': 'private_email_secondary', 'national_ID': 'national_id', 'ext': 'ext',
                'sex': 'sex', 'leaveEndDate': 'leave_end_date', 'officeCity_0': 'office_city_0',
                'host_Country_2': 'host_country_2', 'schoolID': 'school_id', 'jobGradeDesc': 'job_grade_desc',
                'home_Country_2': 'home_country_2', 'cost_Center_Reference_ID_2': 'cost_center_reference_id_2',
                'department_Name': 'department_name', 'pay_Group_0': 'pay_group_0',
                'end_International_Employment_Date_1': 'end_international_employment_date_1',
                'department_Reference_ID_2': 'department_reference_id_2',
                'employee_Contract_Effective_Date': 'employee_contract_effective_date',
                'end_International_Employment_Date_0': 'end_international_employment_date_0',
                'has_International_Assignment': 'has_international_assignment',
                'cost_Center_Code_2': 'cost_center_code_2', 'officeAddressDesc_0': 'office_address_desc_0',
                'coach_Reference_ID': 'coach_reference_id', 'termDate': 'term_date',
                'hire_Reason_Reference': 'hire_reason_reference', 'schoolUniversity': 'school_university',
                'end_Employment_Reason_Reference_1': 'end_employment_reason_reference_1', 'pay_Group_1': 'pay_group_1',
                'job_Profile_Reference_ID_1': 'job_profile_reference_id_1', 'careerCoach': 'career_coach',
                'company_Name_2': 'company_name_2', 'global_Line_of_Service_Name_2': 'global_line_of_service_name_2',
                'gradeCode': 'grade_code', 'mobile_PhoneNo_Primary': 'mobile_phone_no_primary',
                'location_1': 'location_1',
                'global_Line_of_Service_Reference_ID': 'global_line_of_service_reference_id'}

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

tarExistingStaffTableNameDwd = "dwd_existing_staff_day_ef"
tarExistingStaffTableNameOds = "ods_existing_staff_day_ei"
tarExistingStaffTableNameOdsDetail = "ods_existing_staff_detail_day_ei"

tarEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/work_day_stage"
    )
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
        date_obj = datetime.datetime.fromisoformat(date) + datetime.timedelta(hours=8)
        result = date_obj.strftime('%m/%d/%Y %I:%M:%S %p')
    elif len(date) == 4:
        date = f"{date}-01-01T12:00:00.000"
        date_obj = datetime.datetime.fromisoformat(date) + datetime.timedelta(hours=8)
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
    existing_url = "https://pwc-dev.it-cpi010-rt.cpi.cn40.apps.platform.sapcloud.cn/http/vprofile/existingstaff"
    truncateTable(tarEngine, tarExistingStaffTableNameDwd)
    syncApiData(user, pwd, existing_url)
    print(f"data sync complete, max cost time is {maxCostTime}, avg cost time is {avgCostTime}")
