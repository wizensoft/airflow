import sys
import logging
from airflow import DAG
from airflow import models
from airflow.models import Variable
# foo = Variable.get("foo")
# bar = Variable.get("bar", deserialize_json=True)
# baz = Variable.get("baz", default_var=None)
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime, timedelta
from airflow.utils.helpers import chain


default_args = {
    'owner': 'annguk',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['koreablaster@wizensoft.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def get_workflow(**context):
    tasks = {}    
    return list(tasks.keys())

with models.DAG("workflow", default_args=default_args, schedule_interval=timedelta(minutes=1)) as dag:
    # Start workflow    
    wf_start = PythonOperator(task_id='wf_start_task', python_callable=get_workflow, provide_context=True, dag=dag)
    # Status get
    wf_status = BashOperator(task_id='wf_status_task',bash_command='echo wf_status get',dag=dag)
    # End workflow    
    wf_end = BashOperator(task_id='wf_end_task',bash_command='echo wf_end ',dag=dag)

    # instances
    instances = BashOperator(task_id='instances_task',bash_command='echo get instancess',dag=dag)

    # Settings
    settings = BashOperator(task_id='settings_task',bash_command='echo get settings',dag=dag)

    # Status: 상태
    status = BashOperator(task_id='status_task',bash_command='echo get status 상태',dag=dag)
    # 00: 기안
    status_00 = BashOperator(task_id='status_00_task',bash_command='echo get status_00 기안',dag=dag)
    # 01: 현결재 true || false
    status_01 = BashOperator(task_id='status_01_task',bash_command='echo get status 현결재 true || false',dag=dag)

    # Area: 결재영역
    area = BashOperator(task_id='area_task', bash_command='echo get area', dag=dag)
    # 00: 기안회수
    area_00 = BashOperator(task_id='area_00_task', bash_command='echo get area_00 기안회수', dag=dag)    
    # 01: 일반결재
    area_01 = BashOperator(task_id='area_01_task', bash_command='echo get area_01 일반결재', dag=dag)
    # 02: 수신결재
    area_02 = BashOperator(task_id='area_02_task', bash_command='echo get area_02 수신결재', dag=dag)
    # 03: 부서합의
    area_03 = BashOperator(task_id='area_03_task', bash_command='echo get area_03 부서합의', dag=dag)

    # Section: 결재구분
    section = BashOperator(task_id='section_task', bash_command='echo get section 결재구분', dag=dag)
    # 00: 기안결재
    section_00 = BashOperator(task_id='section_00_task', bash_command='echo get section_00 기안결재', dag=dag)
    # 01: 일반결재
    section_01 = BashOperator(task_id='section_01_task', bash_command='echo get section_01 일반결재', dag=dag)
    # 02: 병렬합의
    section_02 = BashOperator(task_id='section_02_task', bash_command='echo get section_02 병렬합의', dag=dag)
    # 03: 순차합의
    section_03 = BashOperator(task_id='section_03_task', bash_command='echo get section_03 순차합의', dag=dag)

    # Process: 결재 프로세스 처리(결재선 & 결재함)
    process = BashOperator(task_id='process_task', bash_command='echo get process 결재 프로세스 처리(결재선 & 결재함)', dag=dag)

    # Event: 결재 이벤트 처리
    event = BashOperator(task_id='event_task', bash_command='echo get event 결재 이벤트 처리', dag=dag)
    # 00: 진행중
    event_00 = BashOperator(task_id='event_00_task', bash_command='echo get event_00 진행중 이벤트 처리', dag=dag)
    # 01: 완료
    event_01 = BashOperator(task_id='event_01_task', bash_command='echo get event_01 완료 이벤트 처리', dag=dag)

    # Notify: 알림 이벤트 처리
    notify = BashOperator(task_id='notify_task', bash_command='echo get notify 알림 이벤트 처리', dag=dag)
    # 00: 이메일
    notify_00 = BashOperator(task_id='notify_00_task', bash_command='echo get notify_00 이메일 알림 처리', dag=dag)
    # 01: 쪽지
    notify_01 = BashOperator(task_id='notify_01_task', bash_command='echo get notify_01 쪽지 알림 처리', dag=dag)
    # 02: 모바일
    notify_02 = BashOperator(task_id='notify_02_task', bash_command='echo get notify_02 모바일 알림 처리', dag=dag)

    # Doc: 문서발번
    doc = BashOperator(task_id='doc_task', bash_command='echo get doc 문서발번 처리', dag=dag)

    # Auth: 권한처리
    # auth = BashOperator(task_id='auth_task', bash_command='echo get auth 권한 처리', dag=dag)

    # Complete: 완료처리
    complete = BashOperator(task_id='complete_task', bash_command='echo get complete 완료처리', dag=dag)

    # Workflow Start
    wf_start >> instances >> settings >> status
    # 결재중이 아니면 완료 처리
    instances >> complete
    
    # 결재상태
    status >> status_00 >> section
    status >> status_01

    # 결재영역 >> 결재구분 >> 결재 프로세스 >> 결재 이벤트
    status_01 >> area >> [area_00, area_01, area_02, area_03] >> section >> [section_00, section_01, section_02, section_03] >> process >> event

    # 결재 이벤트 >> 대상자 알림 >> 결재 엔진 상태 종료
    event >> [event_00, event_01] >> complete >> doc >> notify >> [notify_00, notify_01, notify_02] >> wf_end

    totalBuckets = 5

    get_workflow_query = """
    select 
        o.id,
        o.name
    from 
        test o
    """

                

###########################################################################################################

# Generate a set of tasks so we can parallelize the results
# def createOrderProcessingTask(bucket_number):
#     return PythonOperator( 
#                            task_id=f'order_processing_task_{bucket_number}',
#                            python_callable=runOrderProcessing,
#                            pool='order_processing_pool',
#                            op_kwargs={'task_bucket': f'order_processing_task_{bucket_number}'},
#                            provide_context=True,
#                            dag=dag
#                           )


# # Fetch the order arguments from xcom and doStuff() to them
# def runOrderProcessing(task_bucket, **context):
#     orderList = context['ti'].xcom_pull(task_ids='get_open_orders', key=task_bucket)

#     if orderList is not None:
#         for order in orderList:
#             logging.info(f"Processing Order with Order ID {order[order_id]}, customer ID {order[customer_id]}")
#             doStuff(**op_kwargs)


# Discover the orders we need to run and group them into buckets for processing
# def getWorkflows(**context):
#     sql = "select * from test;"
#     db = MySqlHook(mysql_conn_id='mariadb')

#     # initialize the task list buckets
#     tasks = {}
#     # for task_number in range(0, totalBuckets):
#     #     tasks[f'order_processing_task_{task_number}'] = []

#     # populate the task list buckets
#     # distribute them evenly across the set of buckets
#     resultCounter = 0
#     for record in db.get_records(sql):

#         resultCounter += 1
#         bucket = 1 #(resultCounter % totalBuckets)

#         tasks[f'order_processing_task_{bucket}'].append({'order_id': str(record[0]), 'customer_id': str(record[1])})

#     # Push the order lists into xcom
#     # for task in tasks:
#     #     if len(tasks[task]) > 0:
#     #         logging.info(f'Task {task} has {len(tasks[task])} orders.')
#     #         context['ti'].xcom_Push(key=task, value=tasks[task])
#     #     else:
#     #         # if we didn't have enough tasks for every bucket
#     #         # don't bother running that task - remove it from the list
#     #         logging.info(f"Task {task} doesn't have any orders.")
#     #         del(tasks[task])

#     return list(tasks.keys())
