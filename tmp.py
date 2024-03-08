import pandas as pd
import pyodbc
import urllib
from sqlalchemy import text
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import json
from urllib.parse import quote_plus as urlquote


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
    if "CN" == country_code:
        item["holidayFlag"] = 0
        item["workHours"] = 0.0
        item["loading"] = 0
    elif "HK" == country_code:
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


def run():
    item = {}
    result = []
    start_date = datetime.strptime("2023-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("2023-01-02", "%Y-%m-%d")
    # 将开始时间和结束时间转换为浮点数
    start_date_time = convert_hour_to_float(datetime.strptime("2023-01-01 11:30:00", "%Y-%m-%d %H:%M:%S"))
    end_date_time = convert_hour_to_float(datetime.strptime("2023-01-01 13:00:00", "%Y-%m-%d %H:%M:%S"))

    date_diff = abs(start_date - end_date)
    if date_diff.days > 1:
        print("日期差大于一天")
        print("先处理开始那天和结束那天")
        start_date_str = start_date.strftime("%Y-%m-%d")
        work_hour = calculate_hour(start_date_time, 17.5)
        add_item_for_result(start_date_str, item, '', 100, result, work_hour)
        start_date_str = end_date.strftime("%Y-%m-%d")
        work_hour = calculate_hour(9, end_date_time)
        add_item_for_result(start_date_str, item, '', 100, result, work_hour)
        print("处理中间的时间日期")
        start_date_new = start_date + timedelta(days=1)
        end_date_new = end_date - timedelta(days=1)
        start_date_str = start_date_new.strftime("%Y-%m-%d")
        add_items_for_result(start_date_new, end_date_new, start_date_str, item, '', 100, result)

    elif date_diff.days == 1:
        print("日期差等于一天")
        start_date_str = start_date.strftime("%Y-%m-%d")
        work_hour = calculate_hour(start_date_time, 17.5)
        add_item_for_result(start_date_str, item, '', 100, result, work_hour)
        start_date_str = end_date.strftime("%Y-%m-%d")
        work_hour = calculate_hour(9, end_date_time)
        add_item_for_result(start_date_str, item, '', 100, result, work_hour)
    else:
        print("都是当天")
        start_date_str = end_date.strftime("%Y-%m-%d")
        work_hour = calculate_hour(start_date_time, end_date_time)
        add_item_for_result(start_date_str, item, '', 100, result, work_hour)

    print(json.dumps(result, indent=4, ensure_ascii=False))


if __name__ == '__main__':
    run()
