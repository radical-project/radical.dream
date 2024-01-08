# Azure ACI (Azure Container Instance) example

from hydraa.services import CaasManager
from hydraa import proxy, AZURE, Task, AzureVM

provider_mgr = proxy([AZURE])

vm = AzureVM(launch_type='ACI', instance_id='Standard_B1s', min_count=1, max_count=1)
caas_mgr = CaasManager(provider_mgr, [vm], asynchronous=False)

# create 10 tasks and submit them as a batch
tasks = []
for i in range(10):
    task = Task()
    task.memory = 7
    task.vcpus  = 1
    task.provider = AZURE
    task.image  = "screwdrivercd/noop-container"
    task.cmd    = ['/bin/echo', 'Hello Azure ACI task']
    tasks.append(task)

caas_mgr.submit(tasks)

# wait for all tasks to complete
all(t.result() for t in tasks)
