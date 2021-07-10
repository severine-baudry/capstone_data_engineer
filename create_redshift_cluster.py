import boto3
import botocore
from botocore.exceptions import ClientError
import configparser
import json
import pandas as pd
import time
import argparse

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

class RedshiftCluster :
    
    def read_config_file(self, config_name="capstone.cfg"):
        
        self.config = configparser.ConfigParser()
        self.config.read_file( open(config_name))

        self.KEY                    = self.config.get('AWS','ACCESS_KEY_ID')
        self.SECRET                 = self.config.get('AWS','SECRET_ACCESS_KEY')
        self.DWH_CLUSTER_TYPE       = self.config.get("REDSHIFT","CLUSTER_TYPE")
        self.DWH_NUM_NODES          = self.config.get("REDSHIFT","NUM_NODES")
        self.DWH_NODE_TYPE          = self.config.get("REDSHIFT","NODE_TYPE")

        self.DWH_CLUSTER_IDENTIFIER = self.config.get("REDSHIFT","CLUSTER_IDENTIFIER")
        self.DWH_DB                 = self.config.get("REDSHIFT","DB")
        self.DWH_DB_USER            = self.config.get("REDSHIFT","DB_USER")
        self.DWH_DB_PASSWORD        = self.config.get("REDSHIFT","DB_PASSWORD")
        self.DWH_PORT               = self.config.get("REDSHIFT","PORT")

        self.DWH_IAM_ROLE_NAME      = self.config.get("REDSHIFT", "IAM_ROLE_NAME")
        
    def create_resource_clients(self):
        '''
        create clients for Cloud resources
        '''
        self.ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=self.KEY,
                       aws_secret_access_key=self.SECRET
                    )

        self.s3 = boto3.resource('s3',
                            region_name="us-west-2",
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET
                        )

        self.iam = boto3.client('iam',aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET,
                            region_name='us-west-2'
                        )

        self.redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET
                            )
        
    def create_IAM_role(self):
        '''
        create IAM role to enable the refshift cluster to access S3 bucket in read-only
        '''
        try:
            print("1.1 Creating a new IAM Role") 
            self.dwhRole = self.iam.create_role(
                Path='/',
                RoleName=self.DWH_IAM_ROLE_NAME,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
            )
       # except ClientError as e :
       #     print("Please activate access key in AWS console management")
       #     raise(e)
            
        except Exception as e:
            if "EntityAlreadyExists" in e.args[0] :
                print(e)
            else :
                print(type(e))
                raise(e)

        print("1.2 Attaching Policy")
        
        self.iam.attach_role_policy(RoleName=self.DWH_IAM_ROLE_NAME,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']

        print("1.3 Get the IAM role ARN")
        self.roleArn = self.iam.get_role(RoleName=self.DWH_IAM_ROLE_NAME)['Role']['Arn']

        print(self.roleArn)

    def create_cluster(self):
        '''
        create redshift cluster
        '''
        try:
            response = self.redshift.create_cluster(        
                #HW
                ClusterType=self.DWH_CLUSTER_TYPE,
                NodeType=self.DWH_NODE_TYPE,
                NumberOfNodes=int(self.DWH_NUM_NODES),

                #Identifiers & Credentials
                DBName=self.DWH_DB,
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                MasterUsername=self.DWH_DB_USER,
                MasterUserPassword=self.DWH_DB_PASSWORD,
                
                #Roles (for s3 access)
                IamRoles=[self.roleArn]  
            )
        except Exception as e:
            print(e)
            
        status = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']
        i=0
        # wait until the cluster creation is complete before getting endpoint and ARN
        while status != 'available':
            # It ususally takes around 4 minutes (240 seconds) to create a redshift cluster : be patient !
            print(f"Cluster creation in progress for {10*i} seconds")
            time.sleep(10)
            i+=1
            status = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']
        
        myClusterProps = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        self.DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        self.DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        print(f"cluster endpoint: {self.DWH_ENDPOINT}")
        print(f"cluster ARN: {self.DWH_ROLE_ARN}")
    
    def print_cluster_properties(self):
        myClusterProps = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        prettyRedshiftProps(myClusterProps)
            
    def delete_cluster(self):
        self.redshift.delete_cluster( ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        self.iam.detach_role_policy(RoleName=self.DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        self.iam.delete_role(RoleName=self.DWH_IAM_ROLE_NAME)

        
    def execute(self, config_name="capstone.cfg"):
        print("READING CONFIG FILE")
        self.read_config_file(config_name)
        print("CREATE RESOURCE CLIENTS")
        self.create_resource_clients()
        print("CREATE IAM")
        self.create_IAM_role()
        print("CREATE CLUSTER")
        self.create_cluster()
        

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", default = "capstone.cfg")
    
    l_args = parser.parse_args()
    redshift_cluster = RedshiftCluster()
    redshift_cluster.execute(l_args.config)
    
    res_config = configparser.ConfigParser()
    res_config["PATH"] = redshift_cluster.config["PATH"]
    res_config["REDSHIFT"] = {
        "ENDPOINT" : redshift_cluster.DWH_ENDPOINT,
        "ROLE_ARN" : redshift_cluster.DWH_ROLE_ARN,
        "DB" : redshift_cluster.DWH_DB,
        "USER" : redshift_cluster.DWH_DB_USER,
        "PASSWORD" : redshift_cluster.DWH_DB_PASSWORD,
        "PORT" : redshift_cluster.DWH_PORT
        }

    with open("redshift.cfg", "w") as fs :
        res_config.write(fs)
