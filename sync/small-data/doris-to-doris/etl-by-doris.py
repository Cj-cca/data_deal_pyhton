import pandas as pd
import time
from sqlalchemy import text
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote

# FieldMapping = {'country_code': 'CountryCode', 'staff_id': 'StaffID', 'staff_initial': 'StaffInitial',
#                 'staff_name': 'StaffName', 'first_name': 'FirstName', 'last_name': 'LastName', 'div_code': 'DivCode',
#                 'div_name': 'DivName', 'group_code': 'GroupCode', 'group_name': 'GroupName', 'grade_code': 'GradeCode',
#                 'grade_name': 'GradeName', 'job_title': 'JobTitle', 'phone_no': 'PhoneNo',
#                 'office_building': 'OfficeBuilding', 'office_floor': 'OfficeFloor', 'sec_name': 'SecName',
#                 'office_pin': 'OfficePIN', 'login_id': 'LoginID', 'login_context': 'LoginContext',
#                 'power_staff_code': 'PowerStaffCode', 'power_office_code': 'PowerOfficeCode',
#                 'power_group_code': 'PowerGroupCode', 'power_grade_code': 'PowerGradeCode', 'term_flag': 'TermFlag',
#                 'term_date': 'TermDate', 'email': 'Email', 'guid': 'GUID', 'ldap_dist_name': 'LDAP_DistName',
#                 'pwc_entity_id': 'PwCEntityID', 'local_expat_flag': 'localExpatFlag', 'expat_flag': 'ExpatFlag',
#                 'join_date': 'JoinDate', 'is_professional': 'IsProfessional', 'job_code': 'JobCode', 'bu': 'BU',
#                 'bu_desc': 'BUDesc', 'sub_service': 'SubService', 'sub_service_desc': 'SubServiceDesc',
#                 'inet_email': 'INetEmail', 'notes_id': 'NotesID', 'hr_id': 'HRID', 'native_name': 'NativeName',
#                 'full_name': 'FullName', 'name_in_eng': 'NameInEng', 'given_name': 'GivenName',
#                 'preferred_name': 'PreferredName', 'territory_effective_date': 'TerritoryEffectiveDate',
#                 'active_flag': 'ActiveFlag'}


# FieldMapping = {'guid': 'GUID', 'email_address': 'EmailAddress', 'login_id': 'LoginID', 'ad_name': 'ADName',
#                 'country': 'Country', 'id': 'ID', 'create_date': 'CreateDate', 'modify_date': 'ModifyDate'}


FieldMapping = {"ProcurementRequestNo": "procurement_request_no", "ProcurementURL": "procurement_url",
                "CreateDateTime": "create_date_time", "UpdateDateTime": "update_date_time",
                "ProcurementDtlGuid": "procurement_dtl_guid", "ItemQuantity": "item_quantity", "ItemUnit": "item_unit",
                "ItemPrice": "item_price", "ItemCurrency": "item_currency", "ItemExchangeRate": "item_exchange_rate",
                "ItemVendorDiscount": "item_vendor_discount", "ItemTotalAmount": "item_total_amount",
                "ItemTotalAmountLC": "item_total_amount_lc", "ItemTotalNetAmount": "item_total_net_amount",
                "ItemTotalNetAmountLC": "item_total_net_amount_lc", "OUCode": "ou_code", "OUDesc": "ou_desc",
                "ClientCode": "client_code", "ClientName": "client_name", "JobCode": "job_code", "JobDesc": "job_desc",
                "ProjectCode": "project_code", "ProjectDesc": "project_desc", "CostCategoryCode": "cost_category_code",
                "CostCategoryDesc": "cost_category_desc", "CostTypeCode": "cost_type_code",
                "CostTypeDesc": "cost_type_desc", "SunGLCode": "sun_gl_code", "SunGLDesc": "sun_gl_desc",
                "SubCategoryDesc": "sub_category_desc", "PurchaseItemName": "purchase_item_name",
                "RequestFor": "request_for", "TargetClientFullName": "target_client_full_name",
                "ServiceProvideTo": "service_provide_to", "PIC": "pic", "MIC": "mic",
                "ExpectedRefundDate": "expected_refund_date", "HRCode": "hr_code", "LocationFrom": "location_from",
                "LocationTo": "location_to", "PeriodDateFrom": "period_date_from", "PeriodDateTo": "period_date_to",
                "FiscalYear": "fiscal_year", "ManufacturerName": "manufacturer_name", "OfficeCode": "office_code",
                "OfficeName": "office_name", "RegionCode": "region_code", "CountryCode": "country_code",
                "GroupCode": "group_code", "GroupDesc": "group_desc"}

SourTable = 'ProcurementDetailList'
TarTable = 'ods_fin_procurement_detail_list_hour_ei'


def run(src_engine, tar_engine, table_name):
    sql = text(f"select * from {table_name}")
    srcConn = src_engine.connect()
    tarConn = tar_engine.connect()
    tarConn.execute(text(f"truncate table {table_name}"))
    dataFrame = pd.read_sql(sql, srcConn)
    dataFrame.to_sql(table_name, tarEngine, if_exists='append', index=False)
    print(table_name, "-同步完成")


if __name__ == '__main__':
    table_name_list = "ods_existing_staff_day_ei,ods_hr_api_data_exception_records,ods_hr_job_profiles_day_ei,ods_hr_job_profiles_detail_day_ei,ods_hr_organization_day_ei,ods_hr_organization_detail_day_ei,ods_hr_staff_transfer_from_location_day_ei,ods_hr_staff_transfer_from_location_detail_day_ei,ods_hr_staff_transfer_to_location_day_ei,ods_hr_staff_transfer_to_location_detail_day_ei,ods_hr_task_complete_record,ods_hr_workers_certification_day_ei,ods_hr_workers_certification_detail_day_ei,ods_hr_workers_education_day_ei,ods_hr_workers_education_detail_day_ei,ods_new_hire_or_secondment_day_ei,ods_new_hire_or_secondment_detail_day_ei,ods_termination_and_transfer_day_ei,ods_termination_and_transfer_detail_day_ei"
    srcEngine = create_engine(
        f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/work_day_stage"
    )
    tarEngine = create_engine(f"mysql+pymysql://root:@10.158.16.244:9030/work_day_stage")
    st = time.time()
    for table in table_name_list.split(","):
        run(srcEngine, tarEngine, table)
    print('time : ', time.time() - st)
