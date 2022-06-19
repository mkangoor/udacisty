from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = '',
                 table_name: str = '',
                 sql: str = '',
                 insert_mode: str = 'append',
                 *args, **kwargs) -> None:

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql = sql
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f'Insert rows into {self.table_name} table')
        if self.insert_mode == 'truncate':
            redshift.run(f'TRUNCATE {self.table_name}')
            redshift.run(f'INSERT INTO public.{self.table_name} {self.sql}')
        elif self.insert_mode == 'append':
            redshift.run(f'''
            INSERT INTO public.{self.table_name} 
            {self.sql}
            ''')