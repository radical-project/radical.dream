from hydraa.cloud_vm import vm
from hydraa import providers, services
from hydraa.cloud_task.task import Task
from hydraa import AWS

provider_mgr = providers.proxy([AWS])


fargate_vm = [vm.AwsVM(launch_type='FARGATE')]
caas_mgr = services.manager.CaasManager(provider_mgr,
                                        fargate_vm,
                                        asynchronous=False)

# submit 10 tasks for each vm
tasks = []
for i in range(10):
    task = Task()
    task.memory = 7
    task.vcpus  = 1
    task.ecs_launch_type = 'FARGATE'
    task.provider = AWS
    task.ecs_kwargs = {'subnet': 'subnet-your-subnet-id',
                       'executionRoleArn': 'arn:aws:iam::xxxxxx:role/ecsTaskExecutionRole'}

    task.image  = "screwdrivercd/noop-container"
    task.cmd    = ['/bin/echo', 'hello fargate ecs task']
    tasks.append(task)

caas_mgr.submit(tasks)

# wait for all tasks
all(t.result() for t in tasks)
