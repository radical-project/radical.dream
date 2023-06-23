### Executing Regular Containers on multiple cloud providers
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
### Executing MPI containers:
#### 1- By specifying the setup of the MPI workers and Masters (Launchers): 
```python
from hydraa.services.caas_manager.utils import Kubeflow, KubeflowMPILauncher
mpi_tasks = []
for i in range(5):
    task = Task()
    task.vcpus = 15
    task.memory = 1000
    task.image = 'cylon/cylon-mpi'
    task.cmd = 'python3 mpi_example.py'
    task.provider = JET2
    mpi_tasks.append(task)

# create a kubeflow MPI-launcher
MPIlauncher = KubeflowMPILauncher(num_workers=3, slots_per_worker=7)
kf = Kubeflow(manager=caas_mgr.Jet2Caas).start(launcher=MPIlauncher)

# luanch the containers
kf.launcher.launch_mpi_container(mpi_tasks)
```
#### 2- Or by letting Hydraa setup the MPI workers and Masters automatically:
```python
mpi_tasks = []
for i in range(5):
    task = Task()
    task.vcpus = 15
    task.memory = 1000
    task.image = 'cylon/cylon-mpi'
    task.cmd = 'python3 mpi_example.py'
    task.provider = JET2
    task.type = 'container.mpi'
    mpi_tasks.append(task)
caas_mgr.submit(mpi_tasks)
```

