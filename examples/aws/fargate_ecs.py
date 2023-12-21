from hydraa.cloud_vm import vm
from hydraa import providers, services
from hydraa.cloud_task.task import Task
from hydraa import AWS

provider_mgr = providers.proxy([AWS])


vms = [vm.AwsVM(launch_type='FARGATE')]

# submit 10 tasks for each vm
tasks = []
for i in range(10):
    task = Task()
    task.memory = 7
    task.vcpus  = 1
    task.ecs_launch_type = 'FARGATE'
    task.provider = AWS
    task.ecs_kwargs = {'subnet': 'subnet-0447d4ea0172668d1',
                       'executionRoleArn': 'arn:aws:iam::626113121967:role/ecsTaskExecutionRole'}

    task.image  = "screwdrivercd/noop-container"
    task.cmd    = ['/bin/echo', 'I AM A BARBY GIRL']
    tasks.append(task)
    
caas_mgr.submit(tasks)