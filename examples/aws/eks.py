from hydraa.services import CaasManager
from hydraa import proxy, AWS, Task, AwsVM

provider_mgr = proxy([AWS])

vm = AwsVM(launch_type='EKS', instance_id='m5.4xlarge',
           min_count=8, max_count=8, image_id='ami-xx')

caas_mgr = CaasManager(provider_mgr, [vm], asynchronous=False)

def task_1():
    t = Task(memory=7, vcpus=4, provider=AWS,
             image="your-image", cmd=["python3", "hello_world.py"])
    return t

task_fut = task_1()

caas_mgr.submit(task_fut)

task_fut.result()
