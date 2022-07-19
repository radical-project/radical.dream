## Usage:
#### Amazon AWS Asynchronous mode:
```python
from hydraa import providers, services
from hydraa.cloud_task.task import Task

provider_mgr = providers.proxy()
provider_mgr.login(['aws'])

service_mgr = services.manager
caas_mgr = service_mgr.CaasManager(provider_mgr, asynchronous=True)

tasks = []
for i in range(50):
    CTask = Task()
    CTask.memory = 7
    CTask.vcpus = 1
    CTask.image = "screwdrivercd/noop-container"
    CTask.cmd   = ['/bin/echo', 'noop']
    tasks.append(CTask)
```

```shell
Verifying aws credentials
Login to aws succeed
```

```python
caas_mgr.submit_tasks(tasks, launch_type='EC2', budget = 0.0000016, time = 20)
```

```shell
run cost is higher than budget (0.00076 USD > 1.6e-06 USD), continue? yes/no:
yes
Estimated run_cost is: 0.0008 USD
no cluster found in this account
creating new cluster hydraa_cluster_87587a1d-8dc7-4a10-8b99-08c09cc049e7
ECS cluster hydraa_cluster_87587a1d-8dc7-4a10-8b99-08c09cc049e7 is ACTIVE
No existing EC2 instance found
EC2 instance "i-01e7268f6f34f6756" of type "t2.micro" is being launched
EC2 instance "i-01e7268f6f34f6756" has been started
EC2 instance registered01e7268f6f34f6756 to register
task hydraa_family_11758109-7446-4d32-96f1-478d20253e16 is registered
task hydraa_family_e56c588e-2130-4718-9eb7-8f049a08536b is registered
task hydraa_family_f4bf7e34-67e5-4ec1-a152-edfbcd2c2eb2 is registered
task hydraa_family_5b77ed67-e3b3-4bab-8ad1-9d5e6731e071 is registered
task hydraa_family_16615c71-5957-48ff-8202-9c2e7b06c854 is registered

'9556ead8-6ddd-47b9-ac25-efa829a68c5c'
```

```shell
caas_mgr.AwsCaas.get_run_status('9556ead8-6ddd-47b9-ac25-efa829a68c5c')
run: 9556ead8-6ddd-47b9-ac25-efa829a68c5c is running
pending: 44, running: 3, stopped: 3
```



#### Microsoft Azure Synchronous mode:
```python
In [1]: from hydraa import providers, services
   ...: from hydraa.cloud_task.task import Task

In [2]: provider_mgr = providers.proxy()
   ...: provider_mgr.login(['azure'])
   ...:
   ...: service_mgr = services.manager
```

``` shell
Verifying azure credentials
Login to azure succeed
```

```python
In [3]: caas_mgr = service_mgr.CaasManager(provider_mgr, asynchronous=False)

In [4]: tasks = []
   ...: for i in range(50):
   ...:     CTask = Task()
   ...:     CTask.memory = 0.1
   ...:     CTask.vcpus = 0.1
   ...:     CTask.image = "screwdrivercd/noop-container"
   ...:     CTask.cmd   = ['/bin/echo', 'noop']
   ...:     tasks.append(CTask)
   ...:

In [5]: caas_mgr.submit_tasks(tasks)
```

```shell
Creating resource group 'hydraa-resource-group-6aaa1689-b612-4ada-ba78-f33b17a0e226'...
Pending: 6
Running: 0
Stopped: 44
Finished, 50 tasks stopped with status: "Done"
```

```python
In [9]: caas_mgr.shutdown('azure')
```

```shell
terminating container group hydraa-contianer-group-1c7dfce7-448f-4907-8150-8ea55019e9b7
terminating container group hydraa-contianer-group-f8dd4552-5dc3-438e-95ec-b1bfae1ebd26
terminating container group hydraa-contianer-group-8ba009f5-495c-473d-8aa2-116bc0beef10
terminating container group hydraa-contianer-group-867c1b56-0d95-4a64-b236-de44c990839f
terminating container group hydraa-contianer-group-fac3b677-4cfc-4a89-850b-0ba61f0fd524
terminating container group hydraa-contianer-group-4001e7b2-e176-4198-bb03-2d750cd2a254
terminating container group hydraa-contianer-group-e5369f3f-1cd8-4a7d-a3f4-13b588077574
terminating container group hydraa-contianer-group-163e981b-e396-4898-8c94-78e056ad582d
terminating container group hydraa-contianer-group-87f379b4-af92-4eb7-aa67-aba366b17fdf
terminating resource group hydraa-resource-group-6aaa1689-b612-4ada-ba78-f33b17a0e226
done
```
