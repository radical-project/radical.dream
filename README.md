


### Hydraa supports multiple conccurent inter and cross providers containers execution approaches:
- Azure:
   - Azure Containers Services (ACS)
   - Azure Kuberentes Services (AKS)
- AWS:
   - Elastic Containers Services (ECS):
     - Fargate
     - EC2
   - Elastic Kuberentes Services (EKS)

- Native Kuberentes Cluster deployemnts on multiple nodes

### Executing Regular Containers on multiple cloud providers/services

#### 1- Import Hydraa modules
```python

from hydraa.cloud_vm import vm
from hydraa import providers, services
from hydraa.cloud_task.task import Task
from hydraa import AZURE, AWS, CHI, JET2
```

#### 2- Create the required vms (resources):

```python
provider_mgr = providers.proxy([AZURE, AWS, CHI, JET2])

service_mgr = services.manager

vm0 = vm.AwsVM(launch_type='FARGATE', SubnetID='subnet-xxx',
               IamInstanceProfile={"Arn" : 'arn:aws:iam::xxxxx:instance-profile/ecsInstanceRole',})

vm1 = vm.AwsVM(launch_type='EC2', image_id='ami-xxx',
               min_count=1, max_count=1, instance_id='t2.micro',
               IamInstanceProfile={"Arn" : 'arn:aws:iam::xxxxx:instance-profile/ecsInstanceRole',})

vm2 = vm.AzureVM(launch_type='ACS', instance_id='Standard_B1s', min_count=1, max_count=1)

vm3 = vm.OpenStackVM(provider=JET2, launch_type='KVM', flavor_id='g3.medium',
                      image_id='Featured-Ubuntu20', min_count=2, max_count=2)
                           
vm4 = vm.OpenStackVM(provider=CHI, launch_type='KVM', flavor_id='m1.xlarge',
                        image_id='CC-Ubuntu20.04', min_count=2, max_count=2)

vms = [vm0, vm1, vm2, vm3, vm4]

# start all vms and the requested resources (set asynchronous to True to wait for everything)
caas_mgr = service_mgr.CaasManager(provider_mgr, vms, asynchronous=False, autoterminate=False)
```
#### 4- Hydraa supports 2 mechansims of submssion:
- Batch submission on multiple cloud providers and services:
```python
tasks = []
# submit 1024 tasks (512 for each provider)
for i in range(1024):
    task = Task()
    task.memory = 7
    task.vcpus  = 1
    taks.provider = AWS if i % 2 == 0 else AZURE
    task.image  = "screwdrivercd/noop-container"
    task.cmd    = ['/bin/echo', 'noop']
    tasks.append(task)
caas_mgr.submit(tasks)
```
- Stream submission:
```python
caas_mgr.submit(task)
```

#### 4- Hydraa flexible API and their `Future` based tasks allows users to build their own logic:

##### Create a TF training function:
```python
def train_tf_module():
    task = Task(memory=1024, vcpus=8,
                provider=JET2, image='tf_job_mnist',
                cmd = ['python', '/var/tf_mnist/mnist_with_summaries.py',
                       '--learning_rate=0.01', '--batch_size=150'], set_logs=True)
    return task

caas_mgr.submit(train_tf_module())
```
##### Wait for the task to finish:
```python
train_tf_module.result()
```
##### Create a TF posprocess function:
```python
def post_process_train():
    task = Task(memory=1024, vcpus=2,
                provider=JET2, image='tf_job_mnist',
                cmd = ['python', '/var/tf_mnist/mnist_posprocess.py'], set_logs=True)
    return task

caas_mgr.submit(post_process_train())
```

```python
final_results = post_process_train.result()
```

### Hydraa also support Heterogenious tasks execution:
#### Executing MPI containers: Specify the setup of the MPI workers and Masters (Launchers): 
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

### Hydraa can integrate easily with different executing Workflows engines:
#### 1- create a PVC
```python
from hydraa.services.data.volumes import PersistentVolume, PersistentVolumeClaim
pvc = PersistentVolumeClaim(targeted_cluster=caas_mgr.Jet2Caas.cluster, accessModes='ReadWriteMany')
```
#### 2- create N workflows with `Argo` backend and assign the created PVC to the workflow instance
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
   task.args = []

   task1 = Task()
   task1.vcpus = 2
   task1.memory = 2000
   task1.image = 'facts-fair'
   task1.cmd = ['sh', '-c', f'python3 fair_temperature_fit.py --pipeline_id {i}']
   task1.outputs.append(f'{i}_fit.pkl')
   task1.args = []

   task2 = Task()
   task2.vcpus = 2
   task2.memory = 2000
   task2.image = 'facts-fair'
   task2.cmd = ['sh', '-c', f'python3 fair_temperature_project.py --pipeline_id {i}']
   task2.args = []

   task3 = Task()
   task3.vcpus = 2
   task3.memory = 2000
   task3.image = 'facts-fair'
   task3.cmd = ['sh', '-c', f'python3 fair_temperature_postprocess.py --pipeline_id {i}']
   task3.args = []

   # task2 will wait for task and task1
   # hydraa will move any depndent files
   # from task and task1 to task2
   task2.add_dependency([task, task1])

   wf.add_tasks([task, task1, task2, task3])
   wf.create()

# submit all of the 1000 workflows to the cluster
wf.run()
```

```python
# wait for all workflows to finish
all(t.result() for t in wf.tasks)
```
