from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table_name: str = '',
                 redshift_conn_id: str = '',
                 *args, **kwargs) -> None:

        super(DataQualityOperator, self).__init__(*args, **kwargs) # *args, **kwargs
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table_name}")
        if len(records) < 1 or len(records[0]) < 1:
           raise ValueError(f"Data quality check failed. {self.table_name} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table_name} contained 0 rows")
        logging.info(f"Data quality on table {self.table_name} check passed with {records[0][0]} records")