import logging

from airflow.models import BaseOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class DbDataTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self, db_conn_id='mariadb', db_schema='djob', *args, **kwargs):
        self.db_conn_id = db_conn_id
        self.db_schema = db_schema
        super(DbDataTableOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("이찬호")
        db = MySqlHook(mysql_conn_id=self.db_conn_id, schema=self.db_schema)

        # initialize the task list buckets
        tasks = {}
        index = 0
        rows = db.get_records(get_orders_query)
        for row in rows:        
            index += 1
            tasks[f'order_processing_task_{index}'] = []        
            
        resultCounter = 0
        for row in rows:
            resultCounter += 1
            bucket = (resultCounter % index)
            model = {'id': str(row[0]), 'name': str(row[1])}
            tasks[f'order_processing_task_{bucket}'].append(model)

        # Push the order lists into xcom
        for task in tasks:
            if len(tasks[task]) > 0:
                logging.info(f'Task {task} has {len(tasks[task])} orders.')
                context['ti'].xcom_push(key=task, value=tasks[task])
                
        return list(tasks.values())

class DbDataTablePlugin(AirflowPlugin):
    name = "db_datatable_plugin"
    operators = [DbDataTableOperator]