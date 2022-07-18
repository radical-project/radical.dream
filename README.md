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

In [5]: caas_mgr.execute_ctask_batch(tasks)
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
