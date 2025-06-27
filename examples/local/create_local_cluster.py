from hydraa.services import CaasManager, ServiceManager
from hydraa import proxy, LocalVM, Task, LOCAL

provider_mgr = proxy([LOCAL])

# create a mini-kube local cluster
vm = LocalVM(launch_type='create')

caas_mgr = CaasManager(provider_mgr, [vm], asynchronous=False, auto_terminate=True)

service_mgr = ServiceManager([caas_mgr])
service_mgr.start_services()

@caas_mgr
def multiply(x, y):
    task = Task(memory=128, vcpus=2,
                provider=LOCAL, image='python:3.9.18-slim-bullseye',
                cmd=['python', '-c', f'print({x} * {y})'])

    return task


task = multiply(x=100, y=1200)
task.result()
