import os
import json
import boto3
import pprint
import base64

from src.provider_proxy import proxy
"""
~~~~~~~~~~~~~~~~

A simple script that demonstrates how the docker and AWS Python clients
can be used to automate the process of: building a Docker image, as
defined by the Dockerfile in the project's root directory; pushing the
image to AWS's Elastic Container Registry (ECR); and, then forcing a
redeployment of a AWS Elastic Container Service (ECS) that uses the
image to host the service. 

For now, it is assumed that the AWS infrastructure is already in
existence and that Docker is running on the host machine.
"""


class AwsCaas(object):
    def __init__(self):
        self._ecs_client = None
        self._ec2_client = None

        self._cluster_name = "BotoCluster"
        self._service_name = "service_hello_world"
        self._task_name    = None

    def _deploy_contianer_to_aws(self, cred, container_path):
        """
        Build Docker image, push to AWS and update ECS service.
        """
        self._ecs_client = self._create_ecs_client(cred)
        self._ec2_client = self._create_ec2_client(cred)

        # Create a cluster first (this should be done once)
        self._ecs_client.create_cluster(clusterName=self._cluster_name)

        # create an instance
        self.start_new_cluster()

        # create a task and register it
        self.create_and_register_task_def(1, 400)

        # create the service
        self.create_ecs_service()


    def _create_ec2_client(self, cred):
        # Let's use Amazon EC2
        
        ec2_client = boto3.client('ec2', aws_access_key_id     = cred['aws_access_key_id'],
                                         aws_secret_access_key = cred['aws_secret_access_key'],
                                         region_name           = cred['region_name'])
        
        print('EC2 client created')

        return ec2_client
    

    def _create_ecs_client(self, cred):
        # Let's use Amazon ECS
        ecs_client = boto3.client('ecs', aws_access_key_id     = cred['aws_access_key_id'],
                                         aws_secret_access_key = cred['aws_secret_access_key'],
                                         region_name           = cred['region_name'])
        print('ECS client created')
        return ecs_client
    
    
    def create_ecs_service(self):
        """
        Create service with exactly 1 desired instance of the task
        Info: Amazon ECS allows you to run and maintain a specified number
        (the "desired count") of instances of a task definition
        simultaneously in an ECS cluster.
        """
        response = self._ecs_client.create_service(cluster=self._cluster_name,
                                                   serviceName=self._service_name,
                                                   taskDefinition=self._task_name,
                                                   desiredCount=1,
                                                   clientToken='request_identifier_string',
                                                   deploymentConfiguration={'maximumPercent': 200,
                                                                            'minimumHealthyPercent': 50})
        pprint.pprint(response)
        
        return response


    def create_and_register_task_def(self, cpu, memory):
        import uuid 
        """
        create a container defination
        """

        # FIXME: This a registering with minimal definition,
        #        user should specifiy how much per task (cpu/mem) 
        definitions = dict(family="hello_world",
                           containerDefinitions=[{"name"     : "hello_world",
                                                  "image"    : "hello-world:latest",
                                                  "cpu"      : 1,
                                                  "memory"   : 400,
                                                  "essential": True,}])

        self._ecs_client.register_task_definition(**definitions)

        # FIXME: this should be a uniqe name that the user provides
        #        with the unique uuid
        self._task_name = "hello_world"
        
        print('task {0} submitted'.format(self._task_name))

        return self._task_name


    def list_tasks(self, task_name):
        response = self._ecs_client.list_task_definitions(familyPrefix=self.task_name,
                                                          status='ACTIVE')
        return response

    
    def start_new_cluster(self):
        # Use the official ECS image
        """
        By default, your container instance launches into your default cluster.
        If you want to launch into your own cluster instead of the default,
        choose the Advanced Details list and paste the following script
        into the User data field, replacing your_cluster_name with the name of your cluster.
        !/bin/bash
        echo ECS_CLUSTER=your_cluster_name >> /etc/ecs/ecs.config

        ImageId: An AMI ID is required to launch an instance and must be
                 specified here or in a launch template.

        Instancetype: The instance type. Default m1.small For more information 
                      (https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html).
        
        MinCount: The minimum number of instances to launch.
        MaxCount: The maximum number of instances to launch.

        UserData: accept any linux commnad (acts a bootstraper for the instance)
        """
        cmd = "#!/bin/bash \n echo ECS_CLUSTER=" + self._cluster_name + " >> /etc/ecs/ecs.config"
        response = self._ec2_client.run_instances(ImageId="ami-8f7687e2",
                                                  MinCount=1, MaxCount=1,
                                                  InstanceType="t2.micro",
                                                  UserData= cmd)
        pprint.pprint(response)
        
        return response

    
    def shutdown(self):
        #Shut everything down and delete task/service/instance/cluster
        try:
            # Set desired service count to 0 (obligatory to delete)
            response = self._ecs_client.update_service(cluster=self._cluster_name,
                                                       service=self._service_name,
                                                       desiredCount=0)
            # Delete service
            response = self._ecs_client.delete_service(cluster=self._cluster_name,
                                                       service=self._service_name)
            pprint.pprint(response)
        except:
            print("Service not found/not active")
        
        # List all task definitions and revisions
        tasks = self.list_tasks(self._task_name)

        # De-Register all task definitions
        for task_definition in tasks["taskDefinitionArns"]:
            # De-register task definition(s)
            deregister_response = self._ecs_client.deregister_task_definition(
                taskDefinition=task_definition)
            pprint.pprint(deregister_response)
        
        # Terminate virtual machine(s)
        instances = self._ecs_client.list_container_instances(cluster=self._cluster_name)
        if instances["containerInstanceArns"]:
            container_instance_resp = self._ecs_client.describe_container_instances(
            cluster=self._cluster_name,
            containerInstances=instances["containerInstanceArns"])

            for ec2_instance in container_instance_resp["containerInstances"]:
                ec2_termination_resp = ec2_client.terminate_instances(
                    DryRun=False,
                    InstanceIds=[ec2_instance["ec2InstanceId"],])

        # Finally delete the cluster
        response = ecs_client.delete_cluster(cluster=cluster_name)
        pprint.pprint(response)





    
    


    



        
