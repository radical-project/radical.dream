```python

from hydraa.cloud_vm import vm
from hydraa import providers, services
from hydraa.cloud_task.task import Task
from hydraa import AZURE, AWS, CHI, JET2

provider_mgr = providers.proxy([AZURE, AWS, CHI, JET2])

service_mgr = services.manager

vm1 = vm.AwsVM(launch_type='EC2', image_id='ami-061c10a2cb32f3491', min_count=1, max_count=1, instance_id='t2.micro',
                user_data=None, profile={"Arn" : 'arn:aws:iam::xxxxx:instance-profile/ecsInstanceRole',}

vm2 = vm.AzureVM(launch_type='ACS', instance_id='Standard_B1s', min_count=1, max_count=1)

vm3 = vm.OpenStackVM(provider=JET2, launch_type='KVM', flavor_id='g3.medium',
                      image_id='Featured-Ubuntu20', min_count=2, max_count=2)
                           
vm4 = vm.OpenStackVM(provider=CHI, launch_type='KVM', flavor_id='m1.xlarge',
                        image_id='CC-Ubuntu20.04', min_count=2, max_count=2)

vms = [vm1, vm2, vm3, vm4]

# start all vms and the requested resources (set asynchronous to True to wait for everything)
caas_mgr = service_mgr.CaasManager(provider_mgr, vms, asynchronous=False)
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
  
       simul_task = Task()
       simul_task.memory   = 100 #MB
       simul_task.vcpus    = 2
       simul_task.image    = "xxx/simulate"
       simul_task.args     = result
       simul_task.provider = AWS
       caas_mgr.submit(simul_task)
    
    # wait for the task
    simul_task.done()
    
    print(task.result())
  
```

What About HPC + Cloud?

```python
import os
import radical.pilot as rp
from hydraa.cloud_vm import vm
from hydraa.cloud_task.task import Task
from hydraa.services import service_manager
from hydraa.services.hpc_manager import radical_pilot
from hydraa import AZURE, AWS, JET2, providers, services


os.environ["RADICAL_PILOT_DBURL"] = "mongodb://user:password@ip:port/db_name"

pdsec = rp.PilotDescription()

pdsec.cores = 4
pdsec.runtime = 30
pdsec.access_schema = 'local'
pdsec.resource = 'local.locahost'


hpc_tasks = []
for i in range(4000):
    task = Task()
    task.vcpus  = 1
    task.memory = 7
    task.cmd    = ['/bin/echo noop']
    hpc_tasks.append(task)


hpc_manager = radical_pilot.RadicalPilot(pdsec, hpc_tasks)


provider_mgr = providers.proxy([AZURE, JET2])
vms = [vm.AzureVM(launch_type='AKS', instance_id='Standard_B4ms', min_count=1, max_count=1),
       vm.OpenStackVM(provider=JET2, launch_type='KVM', flavor_id='g3.medium', image_id='Featured-Ubuntu20', min_count=2, max_count=2),]


# 4000 for each provider
cloud_tasks = []
for i in range(8000):
    task = Task()
    task.memory = 7
    task.vcpus  = 1
    task.image  = "screwdrivercd/noop-container"
    task.cmd    = ['/bin/echo', 'noop']
    task.provider = AZURE if i % 2 == 0 else JET2
    cloud_tasks.append(task)


caas_manager = services.manager.CaasManager(provider_mgr, vms, cloud_tasks, asynchronous=False)


managers = [hpc_manager, caas_manager]

service_manager =  service_manager.service_manager(managers)
service_manager.start()
```