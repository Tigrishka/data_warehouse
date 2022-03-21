import os
import configparser
import boto3
from botocore.exceptions import ClientError
import psycopg2
import json
import logging
import argparse

# load DWH params from config file
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get("AWS","KEY")
SECRET                 = config.get("AWS","SECRET")

DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
REGION                 = config.get("CLUSTER", "REGION")
DWH_DB                 = config.get("DB","DB_NAME")
DWH_DB_USER            = config.get("DB","DB_USER")
DWH_DB_PASSWORD        = config.get("DB","DB_PASSWORD")
DWH_PORT               = config.get("DB","DB_PORT")
DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")
S3_READ_ONLY_ARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

#(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

def create_clients():

    """ Create clients for EC2, S3, IAM, and Redshift 
    
    Return:
    ec2, s3, iam, redshift
    """

    ec2 = boto3.resource('ec2', 
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                        )
    s3 = boto3.resource('s3',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET 
                        )
    iam = boto3.client('iam',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET 
                        )
    redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET 
                        )

    return ec2, s3, iam, redshift

def create_iam_role(iam):
    
    """ Create IAM role that makes Redshift able to access S3 bucket (ReadOnly)
    
    Arguments:
    iam
    
    Return:
    role_arn
    """
    
    try:
        print('Creating a new IAM Role')
        dwh_role = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
        
        """Attaching role policy"""

        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn=S3_READ_ONLY_ARN
        )
        
    except ClientError as e:
        logging.warning(e)
        
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    logging.info('Role {} with arn {}'.format(DWH_IAM_ROLE_NAME, role_arn))
    
    return role_arn

def create_redshift_cluster(redshift, role_arn):
    
    """ Create Redshift cluster 
    
    Arguments:
    redshift
    role_arn
    
    Return:
    None
    """
    
    try:
        redshift.create_cluster(
            # add parameters for hardware
            ClusterType=config['CLUSTER']['DWH_CLUSTER_TYPE'],
            NumberOfNodes=int(config['CLUSTER']['DWH_NUM_NODES']),
            NodeType=config['CLUSTER']['DWH_NODE_TYPE'],
            
            # add parameters for identifiers & credentials
            DBName=config['DB']['DB_NAME'],
            ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'],
            MasterUsername=config['DB']['DB_USER'],
            MasterUserPassword=config['DB']['DB_PASSWORD'],
            Port=int(config['DB']['DB_PORT']),
            
            # add parameter for role (to allow s3 access)
            IamRoles=[role_arn]
        )
        logging.info('Creating cluster {}...'.format(DWH_CLUSTER_IDENTIFIER))
        
    except ClientError as e:
        logging.warning(e)

        
def open_tcp(ec2,redshift):
    
    """ Open an incoming TCP port to access the cluster endpoint from outside VPC
    
    Arguments:
    ec2
    redshift
    
    Return:
    None
    """

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        default_sg = list(vpc.security_groups.all())[0]
        print(default_sg)
        
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )
        logging.info('Allow TCP connections from ', CidrIp)
        
    except ClientError as e:
        logging.warning(e)
        

def delete_iam_role(iam):
    
    """ Delete IAM role
    
    Arguments:
    iam
    
    Return:
    None
    """
    
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=S3_READ_ONLY_ARN)
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    logging.info('Deleted role {} with {}'.format(DWH_IAM_ROLE_NAME, role_arn))


def delete_redshift_cluster(redshift):
    
    """ Delete Redshift cluster
    
    Arguments:
    redshift
    
    Return:
    None
    """
    
    try:
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, 
                                SkipFinalClusterSnapshot=True)
        
        logging.info('Deleted cluster {}'.format(DWH_CLUSTER_IDENTIFIER))
        
    except Exception as e:
        logging.error(e)

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete", help="delete iam role and redshift cluster", action="store_true")
    parser.add_argument("--create", help="delete iam role and redshift cluster", action="store_true")

    args = parser.parse_args()
    
    ec2, s3, iam, redshift = create_clients()
    
    if args.delete:
        delete_redshift_cluster(redshift)
        delete_iam_role(iam)
    elif args.create:
        role_arn = create_iam_role(iam)
        create_redshift_cluster(redshift, role_arn)
        open_tcp(ec2,redshift)
    else:
        print('No command selected! Exiting...')
        return 0
        
    
if __name__ == "__main__":
    main()