"""
Hydraa MPI Task Execution Script

This script demonstrates the execution of MPI tasks on an AWS EKS cluster
using the Hydraa library.

Example Usage:
--------------
$ python mpi_tasks.py

"""


from hydraa.cloud_vm import vm
from hydraa.cloud_task.task import Task
from hydraa import providers, services, AWS
from hydraa.services.caas_manager.kubernetes.integrations.kubeflow import KubeflowMPILauncher

provider_mgr = providers.proxy([AWS])

# set heterogeneous vms to start on the AWS EKS cluster
vms = [vm.AwsVM(launch_type='EKS', instance_id='c5a.xlarge',
       image_id='ami-061c10a2cb32f3491', min_count=1, max_count=2),

       vm.AwsVM(launch_type='EKS', instance_id='c6a.xlarge',
       image_id='ami-061c10a2cb32f3491', min_count=1, max_count=2)]


# create caas manager that manages the vms/AKS Cluster
caas_mgr = services.manager.CaasManager(provider_mgr, vms, asynchronous=False)

# create single/multi-node mpi tasks
mpi_tasks = []
for i in range(1):
    task = Task()
    task.vcpus = 60
    task.memory = 200000
    task.image = 'aymenalsaadi/cylon-mpi'
    task.cmd = '/cylon/ENV/bin/python3 /cylon/summit/scripts/cylon_scaling.py -s s -n 1000000'
    task.provider = AWS
    mpi_tasks.append(task)


# create and instantiate mpi launcher on the AKS cluster
mpi_launcher = KubeflowMPILauncher(caas_mgr.AwsCaas)

# launch mpi tasks via the launcher
mpi_launcher.launch(mpi_tasks, num_workers=2, slots_per_worker=120)

# wait for tasks to finish
all(t.result() for t in mpi_tasks)

# get logs and save it to a file (optional)
caas_mgr.AwsCaas.get_pod_logs(mpi_tasks[0], save=True)
