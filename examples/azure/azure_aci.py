# Azure None AKS (ACS)

from hydraa.cloud_vm import vm
from hydraa import providers, services
from hydraa.cloud_task.task import Task
from hydraa import AZURE

provider_mgr = providers.proxy([AZURE])

vm = vm.AzureVM(launch_type='ACS', instance_id='Standard_B1s', min_count=1, max_count=1)
caas_mgr = services.manager.CaasManager(provider_mgr, [vm], asynchronous=False)
# submit 10 tasks for each vm
tasks = []
for i in range(10):
    task = Task()
    task.memory = 7
    task.vcpus  = 1
    task.provider = AZURE
    task.image  = "screwdrivercd/noop-container"
    task.cmd    = ['/bin/echo', 'I AM A BARBY GIRL']
    tasks.append(task)

caas_mgr.submit(tasks)
