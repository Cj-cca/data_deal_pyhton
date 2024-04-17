from playwright.sync_api import sync_playwright, Playwright
from datetime import datetime

# 全局常量
STAFF_ID = 'CN5500000'
TASK_INFO = 'Assure'
TASK_TIME = '8'
TASK_DATE = ''


def handle_popup(popup):
    # 处理弹出窗口
    popups.append(popup)


# 存储弹出窗口的列表
popups = []


def run(playwright: Playwright):
    # 获取当前日期
    current_date = datetime.now().strftime("%m/%d/%Y")
    current_day = datetime.now().day

    # 检查TASK_DATE是否被设置（不为空）
    if TASK_DATE:
        # 如果TASK_DATE不为空，则使用TASK_DATE的值
        task_date = datetime.strptime(TASK_DATE, "%Y-%m-%d")
        current_date = task_date.strftime("%m/%d/%Y")
        current_day = task_date.day

    print("当前日期：", current_date)
    browser = None
    try:
        browser = playwright.chromium.launch()
        context = browser.new_context()
        page = context.new_page()

        # 访问首页
        page.goto('https://sdctimecd.asia.pwcinternal.com/SDC/Public/pgIndex.aspx')
        context.on("popup", handle_popup)

        # 访问任务查询页面
        task_page_url = (f'https://sdctimecd.asia.pwcinternal.com/SDC/Public/pgSearchProject.aspx?RequestType'
                         f'=TimeEntry&RequestTime={current_date}&paraStaffID={STAFF_ID}')
        task_page = context.new_page()
        task_page.goto(task_page_url)
        task_page.locator("#txtCode").fill("06084702")
        task_page.get_by_role("button", name="Search").click()
        task_page.get_by_role("cell", name="G004").click()

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
                    break
            timesheet_page.wait_for_timeout(500)

        # 访问时间表页面
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

        print('finish task')

    except Exception as e:
        print(e)

    browser.close()


with sync_playwright() as playwright:
    run(playwright)
