```python

from hydraa import providers, services
from hydraa.cloud_task.task import Task
from hydraa.cloud_vm import vm

provider_mgr = providers.proxy(['aws', 'azure', 'jetstream2', 'chameleon')

service_mgr = services.manager

vm1 = vm.AwsVM(launch_type='EC2', image_id='ami-061c10a2cb32f3491', min_count=1, max_count=1, instance_id='t2.micro',
                user_data=None, profile={"Arn" : 'arn:aws:iam::xxxxx:instance-profile/ecsInstanceRole',}

vm2 = vm.AzureVM(launch_type='ACS', instance_id='Standard_B1s', min_count=1, max_count=1)
vm3 = vm.OpenStackVM(provider='jetstream2', launch_type='KVM', flavor_id='g3.medium', image_id='Featured-Ubuntu20', min_count=2, max_count=2)
vm4 = vm.OpenStackVM(provider='chameleon', launch_type='KVM', flavor_id='m1.xlarge', image_id='CC-Ubuntu20.04', min_count=2, max_count=2)

vms = [vm1, vm2, vm3, vm4]
```

```python
tasks = Task()

# submit 1024 tasks for each vm
for task in range(1024):
    task = Task()
    task.memory = 7
    task.vcpus  = 1
    task.image  = "screwdrivercd/noop-container"
    task.cmd    = ['/bin/echo', 'noop']
    tasks.append(task)
caas_mgr.submit(tasks)
```

```python
def do_something():
    if task[10].done():
       result = task[10].result()
  
    new_task = Task()
    task.memory   = 100 #MB
    task.vcpus    = 2
    task.image    = "xxx/simulate"
    task.args     = result
    task.provider = 'aws'
    
    # wait for the task
    task.done()
    
    print(task.result())
  
```

