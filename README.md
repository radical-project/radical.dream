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
```python
verifying aws loaded credentials
loading aws credentials
login to aws succeed
ecs client created
ec2 client created
iam client created
cluster BotoCluster already exist
task hello_world is registered
no exisitng service found, creating.....
service service_hello_world created
no existing instance found
instance i-0d2f4c26971c61853 created
running task arn:aws:ecs:us-east-1:626113121967:task-definition/hello_world:54
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PROVISIONING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PROVISIONING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PROVISIONING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PROVISIONING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PROVISIONING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['PENDING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['DEPROVISIONING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['DEPROVISIONING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['DEPROVISIONING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['DEPROVISIONING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['DEPROVISIONING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['DEPROVISIONING']
ECS task status for tasks ['arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b']: ['DEPROVISIONING']
ECS tasks arn:aws:ecs:us-east-1:626113121967:task/BotoCluster/8071a301e1244718af82081347b7a73b STOPPED
Shutting down.....
deregistering task arn:aws:ecs:us-east-1:626113121967:task-definition/hello_world:54
deleting cluster BotoCluster
```
