import sys
import json
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
GLOBALS = 1 # 공통
APPLICATION = 2 # 결재
CODE = 'code'
WORKFLOW = 'workflow'
WORKFLOW_STATE = 'state'
WORKFLOW_START_TASK = 'wf_start_task'
INSTANCE = 'instance'
INSTANCE_TASK = 'instances_task'
BOOKMARK_START = 'start'
BOOKMARK_INSTANCE = 'instance'

# WF 마스터 정보
def get_workflows(**context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="djob")
    
    sql = """
    select
        workflow_process_id,ngen,site_id,application_id,instance_id,schema_id,name,workflow_instance_id,state,retry_count,ready,
        execute_date,created_date,bookmark,version,request,reserved,message
    from
        workflow_process
    where 
        ready > 0 and bookmark = ''
    limit 1
    """
    tasks = {}
    # rowCount = 0
    rows = db.get_records(sql)
    tasks[WORKFLOW] = []
    # for row in rows:
    #     rowCount += 1
    #     tasks[f'order_processing_task_{rowCount}'] = []

    resultCount = 0
    for row in rows:
        resultCount += 1
        model = {
            'workflow_process_id':row[0],'ngen':row[1],'site_id':row[2],'application_id':row[3],'instance_id':row[4],
            'schema_id':row[5],'name':row[6],'workflow_instance_id':row[7],'state':row[8],'retry_count':row[9],'ready':row[10],
            'execute_date':str(row[11]),'created_date':str(row[12]),'bookmark':row[13],'version':row[14],'request':row[15],'reserved':row[16],
            'message':row[17]
        }
        # tasks[f'order_processing_task_{bucket}'] = []
        tasks[WORKFLOW].append(model)

    # items = {}
    # Push the order lists into xcom
    # for task in tasks:
    #     if len(tasks[task]) > 0:
    #         logging.info(f'Task {task} has {len(tasks[task])} orders.')
    context['ti'].xcom_push(key=WORKFLOW, value=tasks[WORKFLOW])
                        
    return list(tasks.values())
# 결재 마스터 정보
def get_instance(id, context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    
    sql = f"""
    select
        instance_id,state,form_id,parent_id,workflow_id,subject,creator_culture,creator_name,group_culture,group_name,is_urgency,is_comment,is_related_document,
        attach_count,summary,re_draft_group_id,sub_proc_group_id,interface_id,created_date,completed_date,
        creator_id,group_id
    from
        instances
    where 
        instance_id={id}
    """
    tasks = {}
    rows = db.get_records(sql)
    tasks[INSTANCE] = []
    resultCount = 0
    for row in rows:
        resultCount += 1
        model = {
            'instance_id':row[0],'state':row[1],'form_id':row[2],'parent_id':row[3],'workflow_id':row[4],
            'subject':row[5],'creator_culture':row[6],'creator_name':row[7],'group_culture': row[8],'group_name':row[9],'is_urgency':row[10],'is_comment':row[11],
            'is_related_document':row[12],'attach_count':row[13],'summary':row[14],'re_draft_group_id':row[15],'sub_proc_group_id':row[16],'interface_id':row[17],
            'created_date':str(row[18]),'completed_date':str(row[19]),
            'creator_id':row[20],'group_id':row[21]
        }
        tasks[INSTANCE].append(model)
    return list(tasks.values())

def get_instances(**context):
    workflows = context['ti'].xcom_pull(task_ids=WORKFLOW_START_TASK, key=WORKFLOW)
    # logging.info(f'Get Workflows {workflows}')
    tasks = {}
    tasks[INSTANCE] = []
    for task in workflows:
        logging.info(f'get_instances workflows {task}')
        # workflow = json.dumps(task)
        instance = get_instance(task['instance_id'], context)
        tasks[INSTANCE].append(instance)
        logging.info(f'get_instances {instance}')
    context['ti'].xcom_push(key=INSTANCE, value=tasks[INSTANCE])

# 코드 
def get_codes():
    logging.info('get_codes')

# 결재선 
def get_signers(id, context):
    logging.info('get_signers')
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")    
    sql = f"""
    select
        instance_id,state,form_id,parent_id,workflow_id,subject,creator_culture,creator_name,group_culture,group_name,is_urgency,is_comment,is_related_document,
        attach_count,summary,re_draft_group_id,sub_proc_group_id,interface_id,created_date,completed_date,
        creator_id,group_id
    from
        instances
    where 
        instance_id={id}
    """
    tasks = {}
    rows = db.get_records(sql)
    tasks[INSTANCE] = []
    resultCount = 0
    for row in rows:
        resultCount += 1
        model = {
            'instance_id':row[0],'state':row[1],'form_id':row[2],'parent_id':row[3],'workflow_id':row[4],
            'subject':row[5],'creator_culture':row[6],'creator_name':row[7],'group_culture': row[8],'group_name':row[9],'is_urgency':row[10],'is_comment':row[11],
            'is_related_document':row[12],'attach_count':row[13],'summary':row[14],'re_draft_group_id':row[15],'sub_proc_group_id':row[16],'interface_id':row[17],
            'created_date':str(row[18]),'completed_date':str(row[19]),
            'creator_id':row[20],'group_id':row[21]
        }
        tasks[INSTANCE].append(model)
    return list(tasks.values())

# 설정 마스터
def get_settings(**context):
    codes = get_codes()

    instances = context['ti'].xcom_pull(task_ids=INSTANCE_TASK, key=INSTANCE)
    for task in instances:
        instance_id = task['instance_id']
        get_signers(instance_id, context)

    logging.info(f'get_settings instances {instances}')

# WF 상태
def get_status(**context):
    workflows = context['ti'].xcom_pull(task_ids=WORKFLOW_START_TASK, key=WORKFLOW)
    tasks = {}
    tasks[INSTANCE] = []
    for task in workflows:
        logging.info(f'get_instances workflows {task}')
        # workflow = json.dumps(task)
        instance = get_instance(task['instance_id'], context)
        tasks[INSTANCE].append(instance)
        logging.info(f'get_instances {instance}')
    context['ti'].xcom_push(key=INSTANCE, value=tasks[INSTANCE])


with models.DAG("workflow", default_args=default_args, schedule_interval=timedelta(minutes=1)) as dag:
    # Start workflow    
    wf_start = PythonOperator(task_id=WORKFLOW_START_TASK, python_callable=get_workflows, provide_context=True, dag=dag)
    # Status get
    wf_status = BashOperator(task_id='wf_status_task',bash_command='echo wf_status get',dag=dag)
    # End workflow    
    wf_end = BashOperator(task_id='wf_end_task',bash_command='echo wf_end ',dag=dag)

    # instances
    instances = PythonOperator(task_id=INSTANCE_TASK,python_callable=get_instances, provide_context=True, dag=dag)

    # Settings
    settings = PythonOperator(task_id='settings_task',python_callable=get_settings, provide_context=True, dag=dag)

    # Status: 상태
    status = PythonOperator(task_id='status_task',python_callable=get_status, provide_context=True, dag=dag)
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