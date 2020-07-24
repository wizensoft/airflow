import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class DbDataTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self, params, *args, **kwargs):
        self.operator_param = params
        super(DbDataTableOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("이찬호")
        log.info('operator_param: %s', self.operator_param)

class DbDataTablePlugin(AirflowPlugin):
    name = "db_datatable_plugin"
    operators = [DbDataTableOperator]