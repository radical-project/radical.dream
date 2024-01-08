# The following workflow executes a diamond workflow via Hydraa API on AWS EKS cluster
#
#   A
#  / \
# B   C
#  \ /
#   D

from hydraa.services import CaasManager
from hydraa import proxy, Task, OpenStackVM, JET2


provider_mgr = proxy([JET2])

vms = [OpenStackVM(launch_type='KVM', instance_id='m3.2xl', min_count=1, max_count=1),
       OpenStackVM(launch_type='KVM', instance_id='m3.xl', min_count=2, max_count=2)]

caas_mgr = CaasManager(provider_mgr, vms, asynchronous=False)


def A():
    task = Task(memory=1024, vcpus=8,
                provider=JET2, image='python:3.9.18-slim-bullseye',
                cmd=['python', '-c', 'print(100 * 100)'])
    return task

a = A()
caas_mgr.submit(a)

# wait for A to finish
a_result = a.result()

def B_C(result):
    task = Task(memory=1024, vcpus=8,
                provider=JET2, image='python:3.9.18-slim-bullseye',
                cmd=['python', '-c', f'import math\n print(math.sin({result}))'])
    return task

b, c = B_C(a_result), B_C(a_result)
# B and C can be submitted concurrently
caas_mgr.submit([b, c])

# wait for B and C to finish
b_c_results = [b.result(), c.result()]


def D(result):
    task = Task(memory=1024, vcpus=8,
                provider=JET, image='python:3.9.18-slim-bullseye',
                cmd=['python', '-c', f'import math\n print(math.sum({result}))'])
    return task

d = D(b_c_results)
caas_mgr.submit(d)

# wait for D to finish
d.result()
