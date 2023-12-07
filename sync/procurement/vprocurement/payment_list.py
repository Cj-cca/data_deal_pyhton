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

selectColumn = ["ProcurementRequestNo", "ProcurementURL", "PaymentRequestNo", "PaymentURL", "PaymentStatus",
                "PaymentRequestDate", "CreateDateTime", "UpdateDateTime", "LastApprovalDate", "Requestor",
                "InvoiceFapiaoStatus", "PaymentOfficeCode", "ProcurementEntity", "PaymentDesc", "VendorType",
                "PaymentCurrency", "TotalPaymentAmt", "VendorDiscount", "TotalNetRequestAmt", "PaymentExchangeRate",
                "LocalCurrency", "TotalNetRequestAmtLC", "TotalNetRequestAmtLCRMB", "TotalNetRequestAmtLCHKD",
                "VoucherNo", "PaymentDate", "PaidCurrency", "PaidAmt", "RequestorID", "RequestorOfficeCode",
                "RequestorGroupCode", "RequestorGroupName", "LastVerifyDateTime", "BookingFY", "BookingMonth"]

fieldMapping = {"ProcurementRequestNo": "procurement_request_no", "ProcurementURL": "procurement_url",
                "PaymentRequestNo": "payment_request_no", "PaymentURL": "payment_url",
                "PaymentStatus": "payment_status", "PaymentRequestDate": "payment_request_date",
                "CreateDateTime": "create_date_time", "UpdateDateTime": "update_date_time",
                "LastApprovalDate": "last_approval_date", "Requestor": "requestor",
                "InvoiceFapiaoStatus": "invoice_fapiao_status", "PaymentOfficeCode": "payment_office_code",
                "ProcurementEntity": "procurement_entity", "PaymentDesc": "payment_desc", "VendorType": "vendor_type",
                "PaymentCurrency": "payment_currency", "TotalPaymentAmt": "total_payment_amt",
                "VendorDiscount": "vendor_discount", "TotalNetRequestAmt": "total_net_request_amt",
                "PaymentExchangeRate": "payment_exchange_rate", "LocalCurrency": "local_currency",
                "TotalNetRequestAmtLC": "total_net_request_amt_lc",
                "TotalNetRequestAmtLCRMB": "total_net_request_amt_lc_rmb",
                "TotalNetRequestAmtLCHKD": "total_net_request_amt_lc_hkd", "VoucherNo": "voucher_no",
                "PaymentDate": "payment_date", "PaidCurrency": "paid_currency", "PaidAmt": "paid_amt",
                "RequestorID": "requestor_id", "RequestorOfficeCode": "requestor_office_code",
                "RequestorGroupCode": "requestor_group_code", "RequestorGroupName": "requestor_group_name",
                "LastVerifyDateTime": "last_verify_date_time", "BookingFY": "booking_fy",
                "BookingMonth": "booking_month"}
tarTableNameOds = "ods_fin_payment_list_hour_ei"

totalDataCount = 0
totalInsertCount = 0
createTime = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
delta = datetime.timedelta(days=1)
search_start = (createTime - delta).strftime("%Y-%m-%dT00:00:00")
search_end = createTime.strftime("%Y-%m-%dT00:00:00")
tarEngine = create_engine(
    f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.35.241:9030/procurement_all"
)
params = {"UpdateDateFrom": search_start, "UpdateDateTo": search_end}
# params = {"UpdateDateFrom": '2023-01-01', "UpdateDateTo": '2024-01-01'}


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
    data_url = "https://vprocurement.asia.pwcinternal.com/vProcurementDataCenter/api/Payment/PaymentList"
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
