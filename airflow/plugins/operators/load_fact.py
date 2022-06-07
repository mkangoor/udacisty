from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = '',
                 table_name: str = '',
                 sql: str = '',
                 *args, **kwargs) -> None:

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name,
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f'Insert rows into {self.table_name} table')
        redshift.run(self.sql)
