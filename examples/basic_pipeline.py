# The following workflow executes a pipeline workflow
# 
#   train_tf_module (X 100)
#         |
#  post_process_train (X 1)


from hydraa.cloud_vm import vm
from hydraa.cloud_task.task import Task
from hydraa import providers, services, AWS

provider_mgr = providers.proxy([AWS])

vms = [vm.AzureVM(launch_type='AKS', instance_id='Standard_A8m_v2', min_count=1, max_count=1),
       vm.AzureVM(launch_type='AKS', instance_id='Standard_A4m_v2', min_count=2, max_count=2)]

caas_mgr = services.manager.CaasManager(provider_mgr, vms, asynchronous=False)

# define a task to train a tensorflow module
def train_tf_module():
    task = Task(memory=1024, vcpus=8,
                provider=AWS, image='tf_job_mnist',
                cmd = ['python', '/var/tf_mnist/mnist_with_summaries.py',
                       '--learning_rate=0.01', '--batch_size=150'], set_logs=True)
    return task

# submit 100 training tasks as a batch
training_tasks = []
for i in range(100):
    t = train_tf_module()
    training_tasks.append(t)

caas_mgr.submit(training_tasks)

# wait for all training tasks and then submit a post processing task
if all(t.result() for t in training_tasks):
    def post_process_train():
        task = Task(memory=1024, vcpus=2,
                    provider=AWS, image='tf_job_mnist',
                    cmd = ['python', '/var/tf_mnist/mnist_posprocess.py'], set_logs=True)
        return task

    caas_mgr.submit(post_process_train())

# wait for thr post processing task to finish
final_results = post_process_train.result()