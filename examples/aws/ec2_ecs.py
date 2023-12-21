from hydraa.cloud_vm import vm 
from hydraa.cloud_task.task import Task
from hydraa import AWS, providers, services

provider_mgr = providers.proxy([AWS])

vm = vm.AwsVM(launch_type='EC2', instance_id='t2.micro', min_count=1, max_count=1,
              image_id='ami-your-ami-id', SubnetId='subnet-you-subnet-id',
              IamInstanceProfile={'Arn': 'arn:aws:iam::xxxxx:instance-profile/ecsInstanceRole'})

caas_mgr = services.manager.CaasManager(provider_mgr, [vm], asynchronous=False, auto_terminate=False)

tasks = []
for i in range(1024):
    task = Task()
    task.memory = 7
    task.vcpus  = 1
    task.provider = AWS
    task.ecs_launch_type = 'EC2'
    task.ecs_kwargs = {'executionRoleArn': 'arn:aws:iam::xxxxxx:role/ecsTaskExecutionRole'}
    task.image  = "screwdrivercd/noop-container"
    task.cmd    = ['/bin/echo', 'hello world ec2 task']
    
    tasks.append(task)


caas_mgr.submit(tasks)
all(t.result() for t in tasks)
