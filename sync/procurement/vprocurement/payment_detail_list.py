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

selectColumn = ["PaymentRequestNo", "PaymentURL", "CreateDateTime", "UpdateDateTime", "PaymentDtlGuid", "ItemQuantity",
                "ItemUnit", "ItemPrice", "ItemCurrency", "ItemTotalAmount", "ItemVendorDiscountLC",
                "ItemTotalNetAmount", "ItemExchangeRate", "ItemTotalAmountLC", "ItemTotalNetAmountLC", "OUCode",
                "OUDesc", "OfficeCode", "OfficeName", "RegionCode", "CountryCode", "GroupCode", "GroupDesc",
                "ClientCode", "ClientName", "JobCode", "JobDesc", "ProjectCode", "ProjectDesc", "CostCategoryCode",
                "CostCategoryDesc", "CostTypeCode", "CostTypeDesc", "SunGLCode", "SunGLDesc", "SubCategoryDesc",
                "PurchaseItemName", "FiscalYear", "ManufacturerName", "RequestFor", "TargetClientFullName",
                "ServiceProvideTo", "PIC", "MIC", "ExpectedRefundDate", "HRCode", "LocationFrom", "LocationTo",
                "PeriodDateFrom", "PeriodDateTo"]

fieldMapping = {"PaymentRequestNo": "payment_request_no", "PaymentURL": "payment_url",
                "CreateDateTime": "create_date_time", "UpdateDateTime": "update_date_time",
                "PaymentDtlGuid": "payment_dtl_guid", "ItemQuantity": "item_quantity", "ItemUnit": "item_unit",
                "ItemPrice": "item_price", "ItemCurrency": "item_currency", "ItemTotalAmount": "item_total_amount",
                "ItemVendorDiscountLC": "item_vendor_discount_lc", "ItemTotalNetAmount": "item_total_net_amount",
                "ItemExchangeRate": "item_exchange_rate", "ItemTotalAmountLC": "item_total_amount_lc",
                "ItemTotalNetAmountLC": "item_total_net_amount_lc", "OUCode": "ou_code", "OUDesc": "ou_desc",
                "OfficeCode": "office_code", "OfficeName": "office_name", "RegionCode": "region_code",
                "CountryCode": "country_code", "GroupCode": "group_code", "GroupDesc": "group_desc",
                "ClientCode": "client_code", "ClientName": "client_name", "JobCode": "job_code", "JobDesc": "job_desc",
                "ProjectCode": "project_code", "ProjectDesc": "project_desc", "CostCategoryCode": "cost_category_code",
                "CostCategoryDesc": "cost_category_desc", "CostTypeCode": "cost_type_code",
                "CostTypeDesc": "cost_type_desc", "SunGLCode": "sun_gl_code", "SunGLDesc": "sun_gl_desc",
                "SubCategoryDesc": "sub_category_desc", "PurchaseItemName": "purchase_item_name",
                "FiscalYear": "fiscal_year", "ManufacturerName": "manufacturer_name", "RequestFor": "request_for",
                "TargetClientFullName": "target_client_full_name", "ServiceProvideTo": "service_provide_to",
                "PIC": "pic", "MIC": "mic", "ExpectedRefundDate": "expected_refund_date", "HRCode": "hr_code",
                "LocationFrom": "location_from", "LocationTo": "location_to", "PeriodDateFrom": "period_date_from",
                "PeriodDateTo": "period_date_to"}
tarTableNameOds = "ods_fin_payment_detail_list_hour_ei"

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


def syncApiData(query_url):
    global totalDataCount
    proxies = requests.utils.getproxies()
    if proxies and 'https' in proxies:
        proxies['https'] = proxies['http']
    start = time.time()
    response = requests.get(url=query_url, params=params, verify=False)
    response_json = response.json()
    cost_time = round(time.time() - start, 2)
    print(f"data select complete, cost {cost_time}s")
    data_frame = pandas.json_normalize(response_json)
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
    insert_count = result_dataframe.to_sql(tarTableNameOds, tarEngine, if_exists='append', index=False)
    if insert_count is None:
        insert_count = 0
        print("insert_count is None")
    totalInsertCount += insert_count
    print(f"{tarTableNameOds}数据插入成功，总行数{len(result_dataframe)}，受影响行数：", insert_count)


def run():
    global totalInsertCount
    data_url = "https://vprocurement.asia.pwcinternal.com/vProcurementDataCenter/api/Payment/PaymentDetailList"
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
        search_start += datetime.timedelta(days=29)
