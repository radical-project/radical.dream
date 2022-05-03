# HYDRAA
Cloud Broker Implementation 

## Usage:
```python
In [2]: from src.provider_proxy.proxy import proxy
   ...: from src.provider_proxy.aws import AWS
   ...:
   ...:
   ...:
   ...: from src.service_proxy.caas_manager.manager import CaasManager
   ...:
   ...: proxy_mgr = proxy()
   ...:
   ...: proxy_mgr.login(['aws'])
   ...:
   ...: aws_cl = AWS(proxy_mgr)
   ...:
   ...: caas_mgr = CaasManager(proxy_mgr)
   ...: caas_mgr.async_execute_container('aws', 1, 400)
```
output:
```shell
verifying aws loaded credentials
loading aws credentials
login to aws succeed
ecs client created
ec2 client created
iam client created
cluster BotoCluster already exist
task hello_world is registered
service service_hello_world already exist on cluster BotoCluster
found running instances:
i-0298ef1e59a6f276e
running task arn:aws:ecs:us-east-1:626113121967:task-definition/hello_world:46
```
