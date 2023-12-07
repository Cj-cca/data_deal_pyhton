# !/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import json
import re
import zlib
import time
import pandas
import requests
import base64
from sqlalchemy import create_engine
from sqlalchemy import text
from urllib.parse import quote_plus as urlquote

selectColumn = ["ProcurementRequestNo", "ProcurementGuid", "ProcurementURL", "ProjectCode", "VendorSelectionCode",
                "ProjectVendorSelectionURL", "VendorCode", "ProcurementStatus", "CreateDateTime", "UpdateDateTime",
                "LastApprovalDate", "Requestor", "RequestorID", "RequestorOfficeCode", "RequestorGroupCode",
                "RequestorGroupName", "ProcurementOfficeCode", "ProcurementEntity", "ChargeType", "ProcurementDesc",
                "ProcurementCurrency", "TotalProcurementAmount", "VendorDiscount", "TotalNetRequestAmount",
                "ProcurementExchangeRate", "LocalCurrency", "TotalNetRequestAmountLC", "TotalNetRequestAmountLCRMB",
                "TotalNetRequestAmountLCHKD", "PaymentCurrency", "PreAgreedExchangeRateFlag", "PreAgreedExchangeRate",
                "VendorPayeeName", "ContractRelatetdType", "VendorVIANumber", "BMPreapprovalNo", "ProcurementReason",
                "TotalPaidAmount", "RefundAmount", "TotalPaymentinProgress", "TotalPendingRequestAmount",
                "BackupPerson", "PaymentRequestNo"]

fieldMapping = {"ProcurementRequestNo": "procurement_request_no", "ProcurementGuid": "procurement_guid",
                "ProcurementURL": "procurement_url", "ProjectCode": "project_code",
                "VendorSelectionCode": "vendor_selection_code",
                "ProjectVendorSelectionURL": "project_vendor_selection_url", "VendorCode": "vendor_code",
                "ProcurementStatus": "procurement_status", "CreateDateTime": "create_date_time",
                "UpdateDateTime": "update_date_time", "LastApprovalDate": "last_approval_date",
                "Requestor": "requestor", "RequestorID": "requestor_id", "RequestorOfficeCode": "requestor_office_code",
                "RequestorGroupCode": "requestor_group_code", "RequestorGroupName": "requestor_group_name",
                "ProcurementOfficeCode": "procurement_office_code", "ProcurementEntity": "procurement_entity",
                "ChargeType": "charge_type", "ProcurementDesc": "procurement_desc",
                "ProcurementCurrency": "procurement_currency", "TotalProcurementAmount": "total_procurement_amount",
                "VendorDiscount": "vendor_discount", "TotalNetRequestAmount": "total_net_request_amount",
                "ProcurementExchangeRate": "procurement_exchange_rate", "LocalCurrency": "local_currency",
                "TotalNetRequestAmountLC": "total_net_request_amount_lc",
                "TotalNetRequestAmountLCRMB": "total_net_request_amount_lc_rmb",
                "TotalNetRequestAmountLCHKD": "total_net_request_amount_lc_hkd", "PaymentCurrency": "payment_currency",
                "PreAgreedExchangeRateFlag": "pre_agreed_exchange_rate_flag",
                "PreAgreedExchangeRate": "pre_agreed_exchange_rate", "VendorPayeeName": "vendor_payee_name",
                "ContractRelatetdType": "contract_relatetd_type", "VendorVIANumber": "vendor_via_number",
                "BMPreapprovalNo": "bm_preapproval_no", "ProcurementReason": "procurement_reason",
                "TotalPaidAmount": "total_paid_amount", "RefundAmount": "refund_amount",
                "TotalPaymentinProgress": "total_payment_in_progress",
                "TotalPendingRequestAmount": "total_pending_request_amount", "BackupPerson": "backup_person",
                "PaymentRequestNo": "payment_request_no"}

tarTableNameOds = "ods_fin_procurement_list_hour_ei"

totalDataCount = 0
totalInsertCount = 0
createTime = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
delta = datetime.timedelta(days=1)
search_start = (createTime - delta).strftime("%Y-%m-%dT00:00:00")
search_end = createTime.strftime("%Y-%m-%dT00:00:00")
tarEngine = create_engine(
    f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/procurement_all"
)
# params = {"UpdateDateFrom": search_start, "UpdateDateTo": search_end}
params = {"UpdateDateFrom": '2023-01-01', "UpdateDateTo": '2024-01-01'}


def syncApiData(query_url):
    global totalDataCount
    proxies = requests.utils.getproxies()
    if proxies and 'https' in proxies:
        proxies['https'] = proxies['http']
    start = time.time()
    response = requests.get(url=query_url, params=params, verify=False).json()
    cost_time = round(time.time() - start, 2)
    print(f"data select complete, cost {cost_time}s")
    data_frame = pandas.json_normalize(response)
    if len(data_frame) == 0:
        print(f"当前日期：{search_start}-{search_end},数据条数为：0")
        return
    print(f"当前日期：{search_start}-{search_end},数据条数为：{len(data_frame)}")
    totalDataCount += len(data_frame)
    deal_dwd(data_frame)


def deal_dwd(data_frame):
    global totalInsertCount
    result_dataframe = data_frame[selectColumn]
    result_dataframe.rename(columns=fieldMapping, inplace=True)
    new_column = pandas.Series([createTime] * len(result_dataframe), name='upload_time')
    result_dataframe = pandas.concat([result_dataframe, new_column], axis=1)
    insert_count = result_dataframe.to_sql(tarTableNameOds, tarEngine, if_exists='append', index=False)
    if insert_count is None:
        insert_count = 0
        print("insert_count is None")
    totalInsertCount += insert_count
    print(f"{tarTableNameOds}数据插入成功，总行数{len(result_dataframe)}，受影响行数：", insert_count)


def run():
    global totalInsertCount
    data_url = "https://vprocurement.asia.pwcinternal.com/vProcurementDataCenter/api/Procurement/ProcurementList"
    syncApiData(data_url)
    print(
        f"数据同步完成,应入数据条数：{totalDataCount},实际插入数据条数：{totalInsertCount}")
    totalInsertCount = 0


if __name__ == '__main__':
    startTimeStr = '2017-10-01'
    endTimeStr = '2023-11-01'
    search_start = datetime.datetime.strptime(startTimeStr, "%Y-%m-%d")
    search_end = datetime.datetime.strptime(endTimeStr, "%Y-%m-%d")
    while search_start < search_end:
        tmpStartTime = search_start
        tmpEndTime = tmpStartTime + datetime.timedelta(days=30)
        params["UpdateDateFrom"] = tmpStartTime.strftime("%Y-%m-%d")
        params["UpdateDateTo"] = tmpEndTime.strftime("%Y-%m-%d")
        run()
        search_start += datetime.timedelta(days=30)
