from playwright.sync_api import sync_playwright, Playwright

from datetime import datetime

# 全局常量

STAFF_ID = 'CN553844'

TASK_INFO = 'api data sync'

TASK_TIME = '8'

TASK_DATE_LIST = ['2024-04-15', '2024-04-16']

TASK_CODE = '06084702'

JOB_CODE = 'G004'


def handle_popup(popup):
    # 处理弹出窗口

    popups.append(popup)


# 存储弹出窗口的列表

popups = []


def run(play_wright: Playwright):
    browser = play_wright.chromium.launch()

    context = browser.new_context()

    # 获取当前日期

    current_date = datetime.now().strftime("%m/%d/%Y")

    current_day = datetime.now().day

    if TASK_DATE_LIST and len(TASK_DATE_LIST) > 0:

        for task_date in TASK_DATE_LIST:
            task_date = datetime.strptime(task_date, "%Y-%m-%d")

            current_date = task_date.strftime("%m/%d/%Y")

            current_day = task_date.day

            # 主方法

            handle_sdc_time(current_date, current_day, context)

    else:

        handle_sdc_time(current_date, current_day, context)

    browser.close()


def handle_sdc_time(current_date, current_day, context):
    try:

        print(f"------当前填写日期：{current_date}------")

        page = context.new_page()

        # 访问首页

        page.goto('https://sdctimecd.asia.pwcinternal.com/SDC/Public/pgIndex.aspx')

        context.on("popup", handle_popup)

        # 访问任务查询页面

        print("开始查询Project Info...")

        task_page_url = (f'https://sdctimecd.asia.pwcinternal.com/SDC/Public/pgSearchProject.aspx?RequestType'
                         f'=TimeEntry&RequestTime={current_date}&paraStaffID={STAFF_ID}')

        task_page = context.new_page()

        task_page.goto(task_page_url)

        task_page.locator("#txtCode").fill(TASK_CODE)

        task_page.get_by_role("button", name="Search").click()

        task_page.get_by_role("cell", name=JOB_CODE).click()

        # 获取弹出框

        with context.expect_page() as timesheet_page_info:

            task_page.get_by_role("button", name="OK").click()

        timesheet_page = timesheet_page_info.value

        timesheet_page.on("popup", handle_popup)

        confirm_page = None

        while confirm_page is None:

            # 检查弹出窗口

            for popup in popups:

                if 'pgSelectWorkRequestID' in popup.url:
                    confirm_page = popup

                    popup.locator('#GVWorkRequest > tbody > tr:nth-child(2) > td:nth-child(1)').click()

                    popup.get_by_role("button", name="OK").click()

                    print("任务查询完成！")

                    break

            timesheet_page.wait_for_timeout(500)

        # 访问时间表页面

        print("开始填写Task Info...")

        timesheet_page_url = f'https://sdctimecd.asia.pwcinternal.com/SDC/Public/pgNewTimesheet.aspx?StaffID={STAFF_ID}&TimeSheetDate={current_date}&isReturn=Y'

        timesheet_page = context.new_page()

        timesheet_page.goto(timesheet_page_url)

        timesheet_page.locator("#GVTimeEntryDetail_txtHours_0").fill(TASK_TIME)

        timesheet_page.locator("#GVTimeEntryDetail_txtMemo_0").fill(TASK_INFO)

        timesheet_page.get_by_text("Submit").click()

        # 切换回首页提交任务

        submit_page = context.new_page()

        submit_page.goto('https://sdctimecd.asia.pwcinternal.com/SDC/Public/pgIndex.aspx')

        submit_page.wait_for_load_state()

        submit_page.locator(f"#CalendarView > table > tbody > tr:nth-child(2)>td:nth-child({current_day})").click()

        submit_page.locator("#btnDailySubmit").click()

        print(f"------日期：{current_date} 完成提交------")

    except Exception as e:
        print(e)


with sync_playwright() as playwright:
    run(playwright)
