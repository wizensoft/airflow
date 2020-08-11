# import unittest
# from datetime import datetime
# from airflow import DAG
# from airflow.models import TaskInstance
# from airflow.operators import MultiplyBy5Operator


# class DbDataTableOperator(unittest.TestCase):

#     def test_execute(self):
#         dag = DAG(dag_id='datatable', start_date=datetime.now())
#         task = DbDataTableOperator(params=10, dag=dag, task_id='datatable_task')
#         ti = TaskInstance(task=task, execution_date=datetime.now())
#         result = task.execute(ti.get_template_context())
#         self.assertEqual(result, 50)


# suite = unittest.TestLoader().loadTestsFromTestCase(DbDataTableOperator)
# unittest.TextTestRunner(verbosity=2).run(suite)