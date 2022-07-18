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
```
