"""
HPC App Call     hpc_mean    hpc_mean
                    \          /
Futures              a        a
                      \      /
Cloud App Calls    cloud_multiply
                         |
Future                 result
"""


from hydraa import LOCAL, providers, Task, LocalVM
from hydraa.services import CaasManager, HPCManager, ServiceManager


microk8s_config = '/var/snap/microk8s/current/credentials/controller.config'


# 1- Create the provider manager
provider_mgr = providers.proxy([LOCAL])

# 2- Create CaaS Manager for cloud tasks
cmgr = CaasManager(provider_mgr,
                   asynchronous=False,
                   auto_terminate=False,
                   vms=[LocalVM(launch_type='join',
                        KubeConfigPath=microk8s_config)])

# 3- Create HPC Manager for HPC tasks
hmgr = HPCManager({'cores':4,
                   'runtime':30,
                   'access_schema':LOCAL,
                   'resource':'local.localhost_test'})



# 5- Create the Service Manager (main manager)
smgr = ServiceManager([cmgr, hmgr])
smgr.start_services()

# This task will go to HPC and will execute as Executable
@hmgr
def hpc_mean():
    task = Task(vcpus=1, memory = 1,
                cmd='/bin/echo "scale=2; (10 + 15 + 20 + 25) / 4" | bc')
    return task

# This task will go to Kuberentes and will execute as Pod/Container
@cmgr
def cloud_multiply(x, y):
    task = Task(memory=128, vcpus=2,
                provider=LOCAL, image='python:3.9.18-slim-bullseye',
                cmd=['python', '-c', f'print({x} * {y})'])
    return task

# Invoke both tasks (submit), and by passing HPC task to cloud task, dependency will be
# resolved automatically
a = hpc_mean()
b = cloud_multiply(eval(a.result()),
                   eval(a.result()) * 2)

# Wait for HPC task and once resolved cloud tasks will get executed automatically
print('HPC Mean Result: ', eval(a.result()))
print(f'Cloud Mltiply: {b.result()},', f'Result: {cmgr.LocalCaas.cluster.get_pod_logs(b)}')

# Now shutdown all services
smgr.shutdown_services()
