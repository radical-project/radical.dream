
from hydraa.cloud_vm import vm
from hydraa.cloud_task.task import Task
from hydraa import providers, services, JET2

from hydraa.services.data.volumes import PersistentVolumeClaim
from hydraa.services.caas_manager.kubernetes.integrations.workflows import ContainerSetWorkflow

provider_mgr = providers.proxy([JET2])

vm = vm.OpenStackVM(provider=JET2, launch_type='KVM', flavor_id='g3.medium',
                    image_id='Featured-Ubuntu20', min_count=2, max_count=2)

caas_mgr = services.manager.CaasManager(provider_mgr, [vm], asynchronous=False)


pvc = PersistentVolumeClaim(targeted_cluster=caas_mgr.Jet2Caas.cluster, accessModes='ReadWriteMany')

tasks = []
# Initialize a workflow instance
wf = ContainerSetWorkflow(name='fair-facts', manager=caas_mgr.Jet2Caas, volume=pvc)

# create x 1000 conccurent workflows
for i in range(1000):
    task = Task()
    task.vcpus = 2
    task.memory = 2000
    task.image = 'facts-fair'
    task.cmd = ['sh', '-c', f'python3 fair_temperature_preprocess.py --pipeline_id {i}']
    task.outputs.append(f'{i}_preprocess.pkl')
    task.args = []

    task1 = Task()
    task1.vcpus = 2
    task1.memory = 2000
    task1.image = 'facts-fair'
    task1.cmd = ['sh', '-c', f'python3 fair_temperature_fit.py --pipeline_id {i}']
    task1.outputs.append(f'{i}_fit.pkl')
    task1.args = []

    task2 = Task()
    task2.vcpus = 2
    task2.memory = 2000
    task2.image = 'facts-fair'
    task2.cmd = ['sh', '-c', f'python3 fair_temperature_project.py --pipeline_id {i}']
    task2.args = []

    task3 = Task()
    task3.vcpus = 2
    task3.memory = 2000
    task3.image = 'facts-fair'
    task3.cmd = ['sh', '-c', f'python3 fair_temperature_postprocess.py --pipeline_id {i}']
    task3.args = []

    # task2 will wait for task and task1
    # hydraa will move any depndent files
    # from task and task1 to task2
    task2.add_dependency([task, task1])

    wf.add_tasks([task, task1, task2, task3])
    tasks.extend([task, task1, task2, task3])
    wf.create()

# submit all of the 1000 workflows to the cluster
wf.run()

# wait for all of the tasks to complete
all(t.result() for t in tasks)
