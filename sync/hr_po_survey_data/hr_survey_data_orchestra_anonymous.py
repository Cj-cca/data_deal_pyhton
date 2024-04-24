import time
import pandas as pd
import queue
import threading
import datetime
import concurrent.futures
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote

fieldMap = {'X.U.FEFF.RecipientEmail': 'feff_recipient_email', 'Finished': 'finished', 'X.U.FEFF.Year': 'feff_year',
            'Pulse.Survey.Code': 'pulse_survey_code', 'Question': 'question', 'Answer': 'answer', 'Items': 'items',
            'likert': 'likert',
            'PVP.selection': 'pvp_selection', 'multiple.selection': 'multiple_selection',
            'Question.Type': 'question_type', 'Category': 'category',
            'Category.Sequence': 'category_sequence', 'assemble': 'assemble'}

surveyDataOrchestraAnonymousTableName = "dwd_hr_survey_data_orchestra_anonymous_year"
tarEngine = create_engine(
    f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.15.148:6030/hr_po_survey_data"
)
# 请替换为你的CSV文件路径
file_path = 'C:/Users/Austin J Cheng/Downloads/Survey Data Orchestra Remaining Annonymous.csv'
chunk_size = 10000  # 根据实际情况调整
chunks = pd.read_csv(file_path, chunksize=chunk_size)
max_queue_size = 20
task_queue = queue.Queue(max_queue_size)
create_date = datetime.datetime.now().date()
start = time.time()


def write_data(df):
    try:
        df.rename(columns=fieldMap, inplace=True)
        df['create_date'] = create_date
        df = df.fillna('')
        insert_count = df.to_sql(surveyDataOrchestraAnonymousTableName, tarEngine, if_exists='append', index=False)
        print(f"{surveyDataOrchestraAnonymousTableName}数据插入成功，受影响行数：", insert_count)
    except Exception as e1:
        print(e1)


def task_producer(tq):
    # 在这里生成任务并放入队列
    for chunk in chunks:
        tq.put(chunk)


with concurrent.futures.ThreadPoolExecutor(max_workers=10) as t:
    # 启动任务生产者线程
    producer_thread = threading.Thread(target=task_producer, args=(task_queue,))
    producer_thread.start()
    time.sleep(5)
    while True:
        try:
            item = task_queue.get(timeout=5)  # 设置超时以便在队列为空时跳出循环
            t.submit(write_data, item)
        except queue.Empty:
            break

# 主线程等待线程池中的任务执行完成
t.shutdown(wait=True)
# 主线程等待生产者线程执行完
producer_thread.join()
print(round(time.time() - start, 2))
