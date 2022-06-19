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
        FORMAT AS {}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = '',
                 aws_credentials_id: str = '',
                 s3_bucket: str = '',
                 s3_key: str = '',
                 table_name: str = '',
                 format_enriched: str = '',
                 *args, **kwargs) -> None:

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table_name = table_name
        self.format_enriched = format_enriched

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
            self.format_enriched
        )
        redshift.run(formatted_sql)