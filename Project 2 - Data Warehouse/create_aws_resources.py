import configparser
import boto3
import json
import sys
from time import sleep
import botocore.exceptions as be


def main():
    global credentials
    credentials = get_credentials()
    
    ec2, iam, redshift = create_aws_sessions()
    
    create_redshift_role(iam)
    attach_redshift_role_policy(iam)
    
    redshift_iam_arn = get_role_arn(iam, credentials["iam_role"]["name"])
    save_redshift_role_arn_to_config(iam, redshift_iam_arn)
    
    create_redshift_cluster(redshift, redshift_iam_arn)
    wait_for_cluster_creation(redshift)
    save_redshift_endpoint_to_config(redshift)
    
    open_cluster_tcp_port(redshift, ec2)
    
    print("Done.")
    

def get_credentials():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    
    return {
        section.lower(): {
            key: config[section][key] for key in config[section].keys()
        } for section in config.keys()
    }


def create_aws_sessions():
    aws_session = boto3.session.Session(
        aws_access_key_id=credentials["aws"]["key"],
        aws_secret_access_key=credentials["aws"]["secret"],
        aws_session_token=credentials["aws"]["session_token"],
        region_name=credentials["aws"]["region"]
    )
    
    return (
        aws_session.resource("ec2"),
        aws_session.client("iam"),
        aws_session.client("redshift")
    )
    

def create_redshift_role(iam_client):
    try:
        iam_client.create_role(
            Path="/",
            RoleName=credentials["iam_role"]["name"],
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except be.ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print("IAM role already exists, proceeding")
        else:
            print(f"Unexpected error: {e}")
    except Exception as e:
        print(e)
        sys.exit()
        
        
def attach_redshift_role_policy(iam_client):
    try:
        attach_policy = iam_client.attach_role_policy(
            RoleName=credentials["iam_role"]["name"],
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']

        assert attach_policy == 200, "Error attaching policy to Redshift role."
        
    except Exception as e:
        print(e)
        sys.exit()


def save_redshift_role_arn_to_config(iam_client, arn):    
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    config.set("IAM_ROLE", "ARN", arn)

    with open('dwh.cfg', 'w') as f:
        config.write(f)

    
def get_role_arn(iam_client, role_name):
    return iam_client.get_role(RoleName=role_name)['Role']['Arn']


def create_redshift_cluster(redshift_client, redshift_iam_arn):
    try:
        redshift_client.create_cluster(
            ClusterType=credentials["redshift"]["cluster_type"],
            NodeType=credentials["redshift"]["node_type"],
            NumberOfNodes=int(credentials["redshift"]["num_nodes"]),

            DBName=credentials["cluster"]["db_name"],
            ClusterIdentifier=credentials["redshift"]["cluster_identifier"],
            MasterUsername=credentials["cluster"]["db_user"],
            MasterUserPassword=credentials["cluster"]["db_password"],

            IamRoles=[redshift_iam_arn]
        )
    except be.ClientError as e:
        if e.response['Error']['Code'] == 'ClusterAlreadyExists':
            print("Cluster already exists, proceeding")
        else:
            print(f"Unexpected error: {e}")
    except Exception as e:
        print(e)
        sys.exit()
    

def wait_for_cluster_creation(redshift_client):
    cluster_status = get_cluster_props(redshift_client)["ClusterStatus"]
    print("Waiting for the cluster to be available")
    while cluster_status != "available":
        sleep(60)
        cluster_status = get_cluster_props(redshift_client)["ClusterStatus"]
    
    
def get_cluster_props(redshift_client):
    try:
        return redshift_client.describe_clusters(
            ClusterIdentifier=credentials["redshift"]["cluster_identifier"])['Clusters'][0]
    except Exception as e:
        print(e)


def save_redshift_endpoint_to_config(redshift_client):
    props = get_cluster_props(redshift_client)
    endpoint = props['Endpoint']['Address']
    
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    config.set("CLUSTER", "HOST", endpoint)

    with open('dwh.cfg', 'w') as f:
        config.write(f)
    

def open_cluster_tcp_port(redshift_client, ec2_client):
    props = get_cluster_props(redshift_client)
    try:
        vpc = ec2_client.Vpc(id=props['VpcId'])
        default_security_group = list(vpc.security_groups.all())[0]

        default_security_group.authorize_ingress(
            GroupName=default_security_group.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(credentials["cluster"]["db_port"]),
            ToPort=int(credentials["cluster"]["db_port"])
        )
    except Exception as e:
        print(e)
        
        
if __name__ == "__main__":
    main()
