# HYDRAA
Cloud Broker Implementation 

## Usage:
```python
from hydraa import providers, services

provider_mgr = providers.proxy()
provider_mgr.login(['aws'])

service_mgr = services.manager

caas_mgr = service_mgr.CaasManager(provider_mgr)

verifying aws loaded credentials
loading aws credentials
login to aws succeed


ECS      client created
EC2      client created
IAM      client created
Pricing  client created
EC2      resource created
DynamoDB resource created

caas_mgr.sync_execute_ctask('EC2', 1000, 1, 7, budget=0.15, time = 120)
run cost is higher than budget (0.2 USD > 0.15 USD), continue? yes/no:

yes

Estimated run_cost is: 0.15 USD

no cluster found in this account
creating new cluster hydraa_cluster_3c14e84d-d243-4ba7-a493-979aa919b33d
ECS cluster hydraa_cluster_3c14e84d-d243-4ba7-a493-979aa919b33d is ACTIVE
No existing EC2 instance found
EC2 instance "i-08419f0b810c9d466" of type "t2.micro" has been launched
EC2 instance "i-08419f0b810c9d466" has been started

waiting for instance i-08419f0b810c9d466 to register
waiting for instance i-08419f0b810c9d466 to register
waiting for instance i-08419f0b810c9d466 to register
waiting for instance i-08419f0b810c9d466 to register
waiting for instance i-08419f0b810c9d466 to register
waiting for instance i-08419f0b810c9d466 to register
waiting for instance i-08419f0b810c9d466 to register
waiting for instance i-08419f0b810c9d466 to register
waiting for instance i-08419f0b810c9d466 to register
waiting for instance i-08419f0b810c9d466 to register
waiting for instance i-08419f0b810c9d466 to register

EC2 instance registered

task hydraa_family_145e510b-469b-4359-9a8d-6900f1c9d468 is registered
Pending: 2
Running: 0
Stopped: 8
Done, 10 tasks stopped

exit
Shutting down.....
service not found/not active
deregistering task arn:aws:ecs:us-east-1:626113121967:task-definition/hydraa_family_145e510b-469b-4359-9a8d-6900f1c9d468:1
terminating instance i-08419f0b810c9d466
hydraa cluster hydraa_cluster_3c14e84d-d243-4ba7-a493-979aa919b33d found and deleted
done
```
