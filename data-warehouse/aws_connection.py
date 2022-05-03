import boto3
import json
import configparser

class AWS_connection:
    """
    This class is taking care of creating and establishing connection to AWS Redshift cluster. The sequence of its methods are are follow:
    1. Create clients for AWS services
    2. Create IAM AWS role with policy attached
    3. Create Redshift cluster
    4. Open an incoming TCP port to access the cluster ednpoint
    5. Delete IAM AWS role
    6. Delete Redshift cluster
    """
    def __init__(self, file_name: str, section: str = '', val_list: list = '', region_name: str = '', policy: str = ''):
        """
        Class's contructor, assigning default values.
        """
        # input_section_values parameters
        self.file_name = file_name
        self.section = section
        self.val_list = val_list
        
        # create_aws_service parameters
        self.region_name = region_name
        
        # delete_iam_role
        self.policy = policy
        
    def read_config_file(self) -> configparser.ConfigParser:
        """
        Returns ConfigParser.
        """
        config = configparser.ConfigParser()
        config.read_file(open(self.file_name))
        return config
    
    def get_one_off_value(self, key: str, value: str) -> str:
        """
        Returns given value from key 🔑  in config file.
        """
        config = self.read_config_file()
        return config.get(key, value)
    
    def input_section_values(self, section: str, val_list: list) -> configparser.ConfigParser:
        """
        Insert values to its key in config file.
        """
        config = configparser.ConfigParser()
        config.read_file(open(self.file_name))

        var_list = list(dict(config.items(section)).keys())

        for i,j in zip(var_list, val_list):
            try:
                config.set(section, str.upper(i), j)
            except Exception as e:
                print(e)

        with open(self.file_name, 'w') as configfile:
            config.write(configfile)

        return config
    
    def create_aws_service(self, name_of_service: str, key_id: str, secret_key: str) -> boto3.resources.factory.ServiceResource: 
        """
        Create clients for AWS services for later purposes.
        """
        if name_of_service in ['ec2', 's3']:
            return boto3.resource(
                name_of_service, region_name = self.region_name, aws_access_key_id = key_id, aws_secret_access_key = secret_key
            )
        else:
            return boto3.client(
                name_of_service, region_name = self.region_name, aws_access_key_id = key_id, aws_secret_access_key = secret_key
            )

    def create_iam_role_enriched(self, iam_: boto3.resources.factory.ServiceResource, role_name: str, desc: str, path = '/') -> str:
        """
        Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly).
        """
        try:
            print("1.1 Creating a new IAM Role") 
            dwhRole = iam_.create_role(
                Path = path,
                RoleName = role_name,
                Description = desc,
                AssumeRolePolicyDocument = json.dumps(
                    {
                        'Statement': [{'Action': 'sts:AssumeRole',
                       'Effect': 'Allow',
                       'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'
                    }
                )
            )    
        except Exception as e:
            print(e)

        try:
            print('1.2 Attaching Policy')
            iam.attach_role_policy(
                RoleName = role_name,
                PolicyArn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
            )['ResponseMetadata']['HTTPStatusCode']
        except Exception as e:
            print(e)

        print('1.3 Get the IAM role ARN')
        roleArn = iam_.get_role(RoleName = role_name)['Role']['Arn']

        return roleArn
    
    def create_redshift_cluster(
        """
        Create a Redshift Cluster.
        """
        self, 
        redshift: boto3.resources.factory.ServiceResource, 
        cluster_type: str, 
        node_type: str, 
        num_nodes: int, 
        db_name: str, 
        cluster_identifier: str, 
        db_user: str, 
        db_password: str, 
        roleArn: str
    ) -> [str, str]:
        try:
            response = redshift.create_cluster(        
                # add parameters for hardware
                ClusterType = cluster_type,
                NodeType = node_type,
                NumberOfNodes = int(num_nodes),

                # add parameters for identifiers & credentials
                DBName = db_name,
                ClusterIdentifier = cluster_identifier,
                MasterUsername = db_user,
                MasterUserPassword = db_password,

                # add parameter for role (to allow s3 access)
                IamRoles = [roleArn]  
            )
        except Exception as e:
            print(e)

        print('Cluster creating...\n')
        while redshift.describe_clusters(ClusterIdentifier = cluster_identifier)['Clusters'][0]['ClusterStatus'] == 'creating':
            pass
        print(f"Done! Cluster status is {redshift.describe_clusters(ClusterIdentifier = cluster_identifier)['Clusters'][0]['ClusterStatus']}")

        cluster_props = redshift.describe_clusters(ClusterIdentifier = cluster_identifier)['Clusters'][0]

        return cluster_props['Endpoint']['Address'], cluster_props['IamRoles'][0]['IamRoleArn']
    
    def tcp_port_access(self, cluster_props: dict, dwh_port: str, ec2: boto3.resources.factory.ServiceResource, 
                        ip_address: str = '0.0.0.0/2', ip_protocol: str = 'TCP') -> None:
        """
        Open an incoming TCP port to access the cluster ednpoint
        """
        try:
            vpc = ec2.Vpc(id = cluster_props['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)

            defaultSg.authorize_ingress(
                GroupName = cluster_props['ClusterSubnetGroupName'],
                CidrIp = ip_address,
                IpProtocol = ip_protocol,
                FromPort = int(dwh_port),
                ToPort = int(dwh_port)
            )
            print('Creating TCP port')
        except Exception as e:
            print(f'ERROR: {e}')
            
    def delete_iam_role(self, iam, role_name: str) -> None:
        iam.detach_role_policy(RoleName = role_name, PolicyArn = self.policy)
        return iam.delete_role(RoleName = role_name)

    def delete_redshift_cluster(self, redshift: boto3.resources.factory.ServiceResource, cluster_identifier: str) -> None:
        return redshift.delete_cluster(ClusterIdentifier = cluster_identifier,  SkipFinalClusterSnapshot = True)
    
def main():
    """
    - Create an object to a class AWS_connection with default parameters passed.
    - Insert values to its key in config file.
    - Create clients for AWS services for later purposes.
    - Create a Redshift Cluster.
    - Open an incoming TCP port to access the cluster ednpoint.
    """
    
    aws_conn = AWS_connection(
        file_name = 'dwh.cfg', 
        section = 'CLUSTER', 
        val_list = ['', 'dwh', 'dwhuser', 'Passw0rd', '5439', 'dwgRole_user', 'multi-node', '4', 'dc2.large', 'dwhCluster'],
        region_name = 'us-west-2',
        policy = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )
    
    config = aws_conn.input_section_values(aws_conn.section, aws_conn.val_list)
    
    KEY = aws_conn.get_one_off_value('AWS', 'KEY')
    SECRET = aws_conn.get_one_off_value('AWS', 'SECRET')
    LIST_OF_SERVICES = ['ec2', 's3', 'iam', 'redshift']

    for i in LIST_OF_SERVICES:
        try:
            globals()[i] = aws_conn.create_aws_service(i, KEY, SECRET)
            print(f'{i} is created!\n')
        except Exception as e:
            print(e)
            
    DWH_IAM_ROLE_NAME      = aws_conn.get_one_off_value('CLUSTER', 'DWH_IAM_ROLE_NAME')
    DWH_CLUSTER_TYPE       = aws_conn.get_one_off_value('CLUSTER', 'DWH_CLUSTER_TYPE')
    DWH_NUM_NODES          = aws_conn.get_one_off_value('CLUSTER', 'DWH_NUM_NODES')
    DWH_NODE_TYPE          = aws_conn.get_one_off_value('CLUSTER', 'DWH_NODE_TYPE')
    DWH_CLUSTER_IDENTIFIER = aws_conn.get_one_off_value('CLUSTER', 'DWH_CLUSTER_IDENTIFIER')
    DWH_DB                 = aws_conn.get_one_off_value('CLUSTER', 'DB_NAME')
    DWH_DB_USER            = aws_conn.get_one_off_value('CLUSTER', 'DB_USER')
    DWH_DB_PASSWORD        = aws_conn.get_one_off_value('CLUSTER', 'DB_PASSWORD')
    DWH_PORT               = aws_conn.get_one_off_value('CLUSTER', 'DB_PORT')
    
    roleArn = aws_conn.create_iam_role_enriched(iam, DWH_IAM_ROLE_NAME, 'Allows accesses to all services of AWS on behalf of Udacity DE Nanodegre program.')
    
    DWH_ENDPOINT, DWH_ROLE_ARN = aws_conn.create_redshift_cluster(
        redshift,
        DWH_CLUSTER_TYPE,
        DWH_NODE_TYPE,
        int(DWH_NUM_NODES),
        DWH_DB,
        DWH_CLUSTER_IDENTIFIER,
        DWH_DB_USER,
        DWH_DB_PASSWORD,
        roleArn
    )
    
    config = aws_conn.input_section_values('IAM_ROLE', [DWH_ROLE_ARN])
    config = aws_conn.input_section_values('CLUSTER', [DWH_ENDPOINT])
    
    if redshift.describe_clusters(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['VpcSecurityGroups'][0]['Status'] == 'active':
        print(f'Cluster indentifier {DWH_CLUSTER_IDENTIFIER} exists with status active.')
        pass
    else:
        aws_conn.tcp_port_access(redshift.describe_clusters(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER)['Clusters'][0], ec2)
        
if __name__ == "__main__":
    main()