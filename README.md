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
#### Specify the setup of the MPI workers and Masters (Launchers): 
```python
from hydraa.services.caas_manager.integrations.kubeflow import KubeflowMPILauncher
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
mpi_launcher = KubeflowMPILauncher(caas_mgr.Jet2Caas)
mpi_launcher.launch(mpi_tasks, num_workers=1, slots_per_worker=5)

# wait for all tasks to finish
all(t.result() for t in mpi_tasks)
```

### Executing Workflows:
#### 1- create a PVC
```python
from hydraa.services import PersistentVolume, PersistentVolumeClaim
pvc = PersistentVolumeClaim(targeted_cluster=caas_mgr.Jet2Caas.cluster, accessModes='ReadWriteMany')
```
#### 2- create N workflows and assign the created PVC to the workflow instance
```python
from hydraa.services.caas_manager.integrations.workflows import ContainerSetWorkflow

# Initialize a workflow instance
wf = ContainerSetWorkflow(name='fair-facts', manager=caas_mgr.Jet2Caas, volume=pvc)

# create x 1000 workflows
for i in range(1000):
   task = Task()
   task.vcpus = 2
   task.memory = 2000
   task.image = 'facts-fair'
   task.cmd = ['sh', '-c', f'python3 fair_temperature_preprocess.py --pipeline_id {i}']
   task.outputs.append(f'{i}_preprocess.pkl')
   task.volume = pvc
   task.args = []

   task1 = Task()
   task1.vcpus = 2
   task1.memory = 2000
   task1.image = 'facts-fair'
   task1.cmd = ['sh', '-c', f'python3 fair_temperature_fit.py --pipeline_id {i}']
   task1.outputs.append(f'{i}_fit.pkl')
   task1.volume = pvc
   task1.args = []

   task2 = Task()
   task2.vcpus = 2
   task2.memory = 2000
   task2.image = 'facts-fair'
   task2.cmd = ['sh', '-c', f'python3 fair_temperature_project.py --pipeline_id {i}']
   task2.volume = pvc
   task2.args = []

   task3 = Task()
   task3.vcpus = 2
   task3.memory = 2000
   task3.image = 'facts-fair'
   task3.cmd = ['sh', '-c', f'python3 fair_temperature_postprocess.py --pipeline_id {i}']
   task3.volume = pvc
   task3.args = []

   # task2 will wait for task and task1
   # hydraa will move any depndent files
   # from task and task1 to task2
   task2.add_dependency([task ,task1])

   wf.add_tasks([task, task1, task2, task3])
   wf.create()

# submit all of the 1000 workflows to the cluster
wf.run()
```


