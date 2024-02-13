from hydraa.services import CaasManager
from hydraa import proxy, vm, Task, LOCAL

provider_mgr = proxy([LOCAL])

vm = vm.LocalVM(launch_type='join')

caas_mgr = CaasManager(provider_mgr, [vm], asynchronous=False, auto_terminate=True)

@caas_mgr(provider=LOCAL)
def multiply(x, y):
    task = Task(memory=128, vcpus=2,
                provider=LOCAL, image='python:3.9.18-slim-bullseye',
                cmd=['python', '-c', f'print({x} * {y})'])

    return task


task = multiply(x=100, y=1200)
task.result()
