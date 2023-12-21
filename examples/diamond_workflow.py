# The following workflow executes a diamond workflow via Hydraa API on AWS EKS cluster
# 
#   A
#  / \
# B   C
#  \ /
#   D

from hydraa.cloud_vm import vm
from hydraa.cloud_task.task import Task
from hydraa import providers, services, AWS

provider_mgr = providers.proxy([AWS])

vms = [vm.AzureVM(launch_type='AKS', instance_id='Standard_A8m_v2', min_count=1, max_count=1),
       vm.AzureVM(launch_type='AKS', instance_id='Standard_A4m_v2', min_count=2, max_count=2)]

caas_mgr = services.manager.CaasManager(provider_mgr, vms, asynchronous=False)


def A():
    task = Task(memory=1024, vcpus=8,
                provider=AWS, image='image',
                cmd=['python', '-c', '"100 * 100"'], set_logs=True)
    return task

caas_mgr.submit(A())

# wait for A to finish
A_result = A().result()

def B_C(A_result):
    task = Task(memory=1024, vcpus=8,
                provider=AWS, image='image',
                cmd=['python', '-c', f'import math\n math.sin({A_result})'], set_logs=True)
    return task

# B and C can be submitted concurrently
caas_mgr.submit([A(), B()])

# wait for B and C to finish
B_C_results = [B().result(), C().result()]

def D(B_C_results):
    task = Task(memory=1024, vcpus=8,
                provider=AWS, image='image',
                cmd=['python', '-c', f'import math\n math.sum({B_C_results})'], set_logs=True)
    return task

caas_mgr.submit(D())

# wait for D to finish
D().result()
