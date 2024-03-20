import time
import pandas as pd
import queue
import threading
import datetime
import concurrent.futures
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote

fieldMap = {'X.U.FEFF.RecipientEmail': 'feff_recipient_email',
            'Finished': 'finished',
            'X.U.FEFF.Year': 'feff_year',
            'Pulse.Survey.Code': 'pulse_survey_code',
            'XV.alumni': 'xv_alumni',
            'Gender.x': 'gender',
            'Management.Level.x': 'management_level',
            'Location.x': 'location',
            'Region': 'region',
            'Cost.Center.Name': 'cost_center_name',
            'LoS': 'los',
            'BU': 'bu',
            'Special.Filter': 'special_filter',
            'Market.Sector': 'market_sector',
            'Supervisory.Organisation': 'supervisory_organisation',
            'Time.Type.x': 'time_type',
            'Job.Category.x': 'job_category',
            'Job.Level...Primary.Position.x': 'job_level_primary_position',
            'Tenure': 'tenure',
            'Performance.Rating.convergence': 'performance_rating_convergence',
            'Potential.Placement.y': 'potential_placement',
            'Partner..Director.Pipeline.y': 'partner_director_pipeline',
            'STEM.Job': 'stem_job',
            'STEM.Background': 'stem_background',
            'Worker.s.Manager.x': 'worker_manager',
            'Competency.Network': 'competency_network',
            'Cost.center.ID': 'cost_center_id',
            'Hiring.Type': 'hiring_type',
            'Hire.Date': 'hire_date',
            'order': 'order',
            'Post.Ranking': 'post_ranking',
            'Performance.Rating': 'performance_rating',
            'Region.Sequence': 'region_sequence',
            'Digital.Talents': 'digital_talents'
            }

surveyDataOrchestraAnonymousTableName = "dwd_hr_dimension_choir_anonymous_year"
tarEngine = create_engine(
    f"mysql+pymysql://admin_user:{urlquote('6a!F@^ac*jBHtc7uUdxC')}@10.158.15.148:6030/hr_po_survey_data"
)
# 请替换为你的CSV文件路径
file_path = 'C:/Users/Austin J Cheng/Downloads/Dimension Choir Anonymous.csv'
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
    while True:
        try:
            item = task_queue.get(timeout=1)  # 设置超时以便在队列为空时跳出循环
            t.submit(write_data, item)
        except queue.Empty:
            break

# 主线程等待线程池中的任务执行完成
t.shutdown(wait=True)
# 主线程等待生产者线程执行完
producer_thread.join()
print(round(time.time() - start, 2))
