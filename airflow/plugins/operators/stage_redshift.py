from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    staging_events_copy = """
        COPY {} 
        FROM '{}' 
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}' 
        REGION '{}';
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = '',
                 aws_credentials_id: str = '',
                 s3_bucket: str = '',
                 s3_key: str = '',
                 table_name: str = '',
#                  format_: str = '',
                 format_as: str = '',
                 region: str = '',
                 *args, **kwargs) -> None:

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table_name = table_name
#         self.format_ = format_
        self.format_as = format_as
        self.region = region

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info(f'Clearing data from staging {self.table_name} table if any')
        redshift.run(f'DELETE FROM {self.table_name}')
        
        self.log.info('Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        self.log.info(f'Key is {rendered_key}')
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'
        formatted_sql = StageToRedshiftOperator.staging_events_copy.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
#             self.format_,
            self.format_as,
            self.region
        )
        redshift.run(formatted_sql)
        
        self.log.info(f"Rows {redshift.run('SELECT COUNT(*) FROM public.staging_events')}")
