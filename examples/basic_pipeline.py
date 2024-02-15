# The following workflow executes a pipeline workflow
#
#   train_tf_module
#          |
#         \ /
#  post_process_train

from hydraa.services import CaasManager
from hydraa import proxy, AZURE, Task, AzureVM


provider_mgr = proxy([AZURE])

vms = [AzureVM(launch_type='AKS', instance_id='Standard_A8m_v2', min_count=1, max_count=1),
       AzureVM(launch_type='AKS', instance_id='Standard_A4m_v2', min_count=2, max_count=2)]

caas_mgr = CaasManager(provider_mgr, vms, asynchronous=False)

# define a task to train a tensorflow module
def train_tf_module(batch_size):
    task = Task(memory=1024, vcpus=8,
                provider=AZURE, image='tf_job_mnist',
                cmd = ['python', '/var/tf_mnist/mnist_with_summaries.py',
                       f'--learning_rate=0.01', '--batch_size={batch_size}'])
    return task


# submit 100 training tasks as a batch
training_tasks = []
for i in range(100):
    t = train_tf_module(batch_size=i)
    training_tasks.append(t)

caas_mgr.submit(training_tasks)

# wait for all training tasks
[t.result() for t in training_tasks]

# for non-bulk execution we can use @caas_mgr
# to submit direclty to the caas_manager without
# using caas_mgr.submit
@caas_mgr
def post_process_train():
    task = Task(memory=1024, vcpus=2,
                provider=AZURE, image='tf_job_mnist',
                cmd = ['python', '/var/tf_mnist/mnist_posprocess.py'])
    return task


# wait for the post processing task to finish
post_process = post_process_train.result()

# print the output
print(caas_mgr.AzureCaas.cluster.get_pod_logs(post_process))
