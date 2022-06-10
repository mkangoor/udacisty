import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = '',
                 dq_list: list = {},
                 *args, **kwargs) -> None:

        super(DataQualityOperator, self).__init__(*args, **kwargs) # *args, **kwargs
        self.redshift_conn_id = redshift_conn_id
        self.dq_list = dq_list

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for i in self.dq_list:
            actual_outcome = redshift_hook.get_records(i.get('query'))[0][0]
            expected_outcome = i.get('expected_outcome')
            if actual_outcome != expected_outcome:
                raise ValueError(f"Data quality check for sql statement\n{i.get('query')}\n failed! It returns {actual_outcome} while expected outcome is {expected_outcome} records.")
            logging.info(f"Data quality check for sql statement\n{i.get('query')}\n succssed! It returns {actual_outcome} & expected outcome is {expected_outcome} records.")