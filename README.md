## Usage:
#### Amazon AWS Asynchronous mode:
```python
from hydraa.cloud_vm import vm
from hydraa import providers, services
from hydraa.cloud_task.task import Task

provider_mgr = providers.proxy()
provider_mgr.login(['aws'])

service_mgr = services.manager
caas_mgr = service_mgr.CaasManager(provider_mgr, asynchronous=True)

tasks = []
vm    = vm.AwsVM('ami-061c10a2cb32f3491',
                  1, 1, "t2.micro", None,
                  {"Arn" : 'arn:aws:iam::xxxx:instance-profile/ecsInstanceRole',})
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
caas_mgr.submit_tasks(vm, tasks, launch_type='EC2', budget = 0.0000016, time = 20)
```

```shell
run cost is higher than budget (0.00076 USD > 1.6e-06 USD), continue? yes/no:
yes
Estimated run_cost is: 0.0008 USD
starting run 48e97068-c57b-4de4-9444-187302e1ae7a
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
```

```shell
caas_mgr.AwsCaas.get_run_status('48e97068-c57b-4de4-9444-187302e1ae7a')
run: 48e97068-c57b-4de4-9444-187302e1ae7a is running
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
#### CHI Chameleon:

```export chi vars
source $HOME/app_cred.sc
```

```python
from hydraa.cloud_vm import vm
from hydraa import providers, services
from hydraa.cloud_task.task import Task
service_mgr = services.manager
provider_mgr = providers.proxy(['chameleon'])
caas_mgr = service_mgr.CaasManager(provider_mgr, asynchronous=False)
op_vm = vm.OpenStackVM(launch_type='KVM', flavor_id='m1.large', image_id='CC-Ubuntu20.04')

verifying chameleon credentials
login to chameleon succeed
```

```python
tasks = []
for i in range(3):
    CTask = Task()
    CTask.memory = 7
    CTask.vcpus = 1
    CTask.image = "screwdrivercd/noop-container"
    CTask.cmd   = ['/bin/echo', 'noop']
    tasks.append(CTask)

caas_mgr.submit_tasks(op_vm, tasks)
```

```shell
starting run e8b66aa5-57ee-4edb-9986-9120a9829f98
creating ssh key Pair
Now using KVM@TACC:
URL: https://kvm.tacc.chameleoncloud.org
Location: Austin, Texas, USA
Support contact: help@chameleoncloud.org
creating hydraa-chi-e8b66aa5-57ee-4edb-9986-9120a9829f98
instance is ACTIVE
can not assign ip (machine already has a public ip)
Waiting for SSH connectivity on 129.114.27.37 ...
Connection successful
booting K8s cluster on the remote machine
Virtual Machine Detected
dpkg-query: no packages found matching microk8s
Checking for microk8s:
No microk8s. Setting up microk8s.
microk8s (1.25/stable) v1.25.3 from Canonical** installed
No resources found
No resources found in default namespace.
microk8s is not running. Use microk8s inspect for a deeper inspection.
microk8s is running
high-availability: no
  datastore master nodes: 127.0.0.1:19001
  datastore standby nodes: none
addons:
  enabled:
    ha-cluster           # (core) Configure high availability on the current node
    helm                 # (core) Helm - the package manager for Kubernetes
    helm3                # (core) Helm 3 - the package manager for Kubernetes
  disabled:
    cert-manager         # (core) Cloud native certificate management
    community            # (core) The community addons repository
    dashboard            # (core) The Kubernetes dashboard
    dns                  # (core) CoreDNS
    gpu                  # (core) Automatic enablement of Nvidia CUDA
    host-access          # (core) Allow Pods connecting to Host services smoothly
    hostpath-storage     # (core) Storage class; allocates storage from host directory
    ingress              # (core) Ingress controller for external access
    kube-ovn             # (core) An advanced network fabric for Kubernetes
    mayastor             # (core) OpenEBS MayaStor
    metallb              # (core) Loadbalancer for your Kubernetes cluster
    metrics-server       # (core) K8s Metrics Server for API access to service metrics
    observability        # (core) A lightweight observability stack for logs, traces and metrics
    prometheus           # (core) Prometheus operator for monitoring and logging
    rbac                 # (core) Role-Based Access Control for authorisation
    registry             # (core) Private image registry exposed on localhost:32000
    storage              # (core) Alias to hostpath-storage add-on, deprecated
booting Kuberentes cluster successful
booting K8s cluster on the remote machine
Virtual Machine Detected
dpkg-query: no packages found matching microk8s
Checking for microk8s:
No microk8s. Setting up microk8s.
snap "microk8s" is already installed, see 'snap help refresh'
NAMESPACE     NAME                                           READY   STATUS              RESTARTS   AGE
kube-system   pod/calico-node-f276h                          0/1     Running             0          25s
kube-system   pod/calico-kube-controllers-7b4b7968d4-wsxtn   0/1     ContainerCreating   0          25s

NAMESPACE   NAME                 TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)   AGE
default     service/kubernetes   ClusterIP   10.152.183.1   <none>        443/TCP   35s

NAMESPACE     NAME                         DESIRED   CURRENT   READY   UP-TO-DATE   AVAILABLE   NODE SELECTOR            AGE
kube-system   daemonset.apps/calico-node   1         1         0       1            0           kubernetes.io/os=linux   33s

NAMESPACE     NAME                                      READY   UP-TO-DATE   AVAILABLE   AGE
kube-system   deployment.apps/calico-kube-controllers   0/1     1            0           33s

NAMESPACE     NAME                                                 DESIRED   CURRENT   READY   AGE
kube-system   replicaset.apps/calico-kube-controllers-54c85446d4   0         0         0       29s
kube-system   replicaset.apps/calico-kube-controllers-7b4b7968d4   1         1         0       25s
Name:               hydraa-chi-e8b66aa5-57ee-4edb-9986-9120a9829f98
Roles:              <none>
Labels:             beta.kubernetes.io/arch=amd64
                    beta.kubernetes.io/os=linux
                    kubernetes.io/arch=amd64
                    kubernetes.io/hostname=hydraa-chi-e8b66aa5-57ee-4edb-9986-9120a9829f98
                    kubernetes.io/os=linux
                    microk8s.io/cluster=true
                    node.kubernetes.io/microk8s-controlplane=microk8s-controlplane
Annotations:        node.alpha.kubernetes.io/ttl: 0
                    projectcalico.org/IPv4Address: 10.56.2.192/22
                    projectcalico.org/IPv4VXLANTunnelAddr: 10.1.87.0
                    volumes.kubernetes.io/controller-managed-attach-detach: true
CreationTimestamp:  Wed, 02 Nov 2022 22:25:56 +0000
Taints:             <none>
Unschedulable:      false
Lease:
  HolderIdentity:  hydraa-chi-e8b66aa5-57ee-4edb-9986-9120a9829f98
  AcquireTime:     <unset>
  RenewTime:       Wed, 02 Nov 2022 22:26:27 +0000
Conditions:
  Type                 Status  LastHeartbeatTime                 LastTransitionTime                Reason                       Message
  ----                 ------  -----------------                 ------------------                ------                       -------
  NetworkUnavailable   False   Wed, 02 Nov 2022 22:26:25 +0000   Wed, 02 Nov 2022 22:26:25 +0000   CalicoIsUp                   Calico is running on this node
  MemoryPressure       False   Wed, 02 Nov 2022 22:26:27 +0000   Wed, 02 Nov 2022 22:25:56 +0000   KubeletHasSufficientMemory   kubelet has sufficient memory available
  DiskPressure         False   Wed, 02 Nov 2022 22:26:27 +0000   Wed, 02 Nov 2022 22:25:56 +0000   KubeletHasNoDiskPressure     kubelet has no disk pressure
  PIDPressure          False   Wed, 02 Nov 2022 22:26:27 +0000   Wed, 02 Nov 2022 22:25:56 +0000   KubeletHasSufficientPID      kubelet has sufficient PID available
  Ready                True    Wed, 02 Nov 2022 22:26:27 +0000   Wed, 02 Nov 2022 22:26:27 +0000   KubeletReady                 kubelet is posting ready status. AppArmor enabled
Addresses:
  InternalIP:  10.56.2.192
  Hostname:    hydraa-chi-e8b66aa5-57ee-4edb-9986-9120a9829f98
Capacity:
  cpu:                4
  ephemeral-storage:  38695164Ki
  hugepages-1Gi:      0
  hugepages-2Mi:      0
  memory:             8148188Ki
  pods:               110
Allocatable:
  cpu:                4
  ephemeral-storage:  37646588Ki
  hugepages-1Gi:      0
  hugepages-2Mi:      0
  memory:             8045788Ki
  pods:               110
System Info:
  Machine ID:                 fd5814d3b95b44ffa9380e6b012c08aa
  System UUID:                fd5814d3-b95b-44ff-a938-0e6b012c08aa
  Boot ID:                    6fd26db0-fbe7-48ee-b472-e08121893dbb
  Kernel Version:             5.4.0-124-generic
  OS Image:                   Ubuntu 20.04.4 LTS
  Operating System:           linux
  Architecture:               amd64
  Container Runtime Version:  containerd://1.6.6
  Kubelet Version:            v1.25.3
  Kube-Proxy Version:         v1.25.3
Non-terminated Pods:          (2 in total)
  Namespace                   Name                                        CPU Requests  CPU Limits  Memory Requests  Memory Limits  Age
  ---------                   ----                                        ------------  ----------  ---------------  -------------  ---
  kube-system                 calico-node-f276h                           250m (6%)     0 (0%)      0 (0%)           0 (0%)         25s
  kube-system                 calico-kube-controllers-7b4b7968d4-wsxtn    0 (0%)        0 (0%)      0 (0%)           0 (0%)         25s
Allocated resources:
  (Total limits may be over 100 percent, i.e., overcommitted.)
  Resource           Requests   Limits
  --------           --------   ------
  cpu                250m (6%)  0 (0%)
  memory             0 (0%)     0 (0%)
  ephemeral-storage  0 (0%)     0 (0%)
  hugepages-1Gi      0 (0%)     0 (0%)
  hugepages-2Mi      0 (0%)     0 (0%)
Events:
  Type     Reason                   Age              From             Message
  ----     ------                   ----             ----             -------
  Normal   Starting                 33s              kube-proxy
  Normal   Starting                 34s              kubelet          Starting kubelet.
  Warning  InvalidDiskCapacity      34s              kubelet          invalid capacity 0 on image filesystem
  Normal   NodeHasSufficientMemory  34s              kubelet          Node hydraa-chi-e8b66aa5-57ee-4edb-9986-9120a9829f98 status is now: NodeHasSufficientMemory
  Normal   NodeHasNoDiskPressure    34s              kubelet          Node hydraa-chi-e8b66aa5-57ee-4edb-9986-9120a9829f98 status is now: NodeHasNoDiskPressure
  Normal   NodeHasSufficientPID     34s              kubelet          Node hydraa-chi-e8b66aa5-57ee-4edb-9986-9120a9829f98 status is now: NodeHasSufficientPID
  Normal   NodeAllocatableEnforced  34s              kubelet          Updated Node Allocatable limit across pods
  Normal   RegisteredNode           29s              node-controller  Node hydraa-chi-e8b66aa5-57ee-4edb-9986-9120a9829f98 event: Registered Node hydraa-chi-e8b66aa5-57ee-4edb-9986-9120a9829f98 in Controller
  Normal   NodeReady                3s               kubelet          Node hydraa-chi-e8b66aa5-57ee-4edb-9986-9120a9829f98 status is now: NodeReady
  Warning  MissingClusterDNS        2s (x2 over 3s)  kubelet          kubelet does not have ClusterDNS IP configured and cannot create Pod using "ClusterFirst" policy. Falling back to "Default" policy.
microk8s is running
high-availability: no
  datastore master nodes: 127.0.0.1:19001
  datastore standby nodes: none
addons:
  enabled:
    ha-cluster           # (core) Configure high availability on the current node
    helm                 # (core) Helm - the package manager for Kubernetes
    helm3                # (core) Helm 3 - the package manager for Kubernetes
  disabled:
    cert-manager         # (core) Cloud native certificate management
    community            # (core) The community addons repository
    dashboard            # (core) The Kubernetes dashboard
    dns                  # (core) CoreDNS
    gpu                  # (core) Automatic enablement of Nvidia CUDA
    host-access          # (core) Allow Pods connecting to Host services smoothly
    hostpath-storage     # (core) Storage class; allocates storage from host directory
    ingress              # (core) Ingress controller for external access
    kube-ovn             # (core) An advanced network fabric for Kubernetes
    mayastor             # (core) OpenEBS MayaStor
    metallb              # (core) Loadbalancer for your Kubernetes cluster
    metrics-server       # (core) K8s Metrics Server for API access to service metrics
    observability        # (core) A lightweight observability stack for logs, traces and metrics
    prometheus           # (core) Prometheus operator for monitoring and logging
    rbac                 # (core) Role-Based Access Control for authorisation
    registry             # (core) Private image registry exposed on localhost:32000
    storage              # (core) Alias to hostpath-storage add-on, deprecated
microk8s is running
high-availability: no
  datastore master nodes: 127.0.0.1:19001
  datastore standby nodes: none
addons:
  enabled:
    ha-cluster           # (core) Configure high availability on the current node
    helm                 # (core) Helm - the package manager for Kubernetes
    helm3                # (core) Helm 3 - the package manager for Kubernetes
  disabled:
    cert-manager         # (core) Cloud native certificate management
    community            # (core) The community addons repository
    dashboard            # (core) The Kubernetes dashboard
    dns                  # (core) CoreDNS
    gpu                  # (core) Automatic enablement of Nvidia CUDA
    host-access          # (core) Allow Pods connecting to Host services smoothly
    hostpath-storage     # (core) Storage class; allocates storage from host directory
    ingress              # (core) Ingress controller for external access
    kube-ovn             # (core) An advanced network fabric for Kubernetes
    mayastor             # (core) OpenEBS MayaStor
    metallb              # (core) Loadbalancer for your Kubernetes cluster
    metrics-server       # (core) K8s Metrics Server for API access to service metrics
    observability        # (core) A lightweight observability stack for logs, traces and metrics
    prometheus           # (core) Prometheus operator for monitoring and logging
    rbac                 # (core) Role-Based Access Control for authorisation
    registry             # (core) Private image registry exposed on localhost:32000
    storage              # (core) Alias to hostpath-storage add-on, deprecated
booting Kuberentes cluster successful
pod/hydraa-pod-e8b66aa5-57ee-4edb-9986-9120a9829f98 created
NAME                                              READY   STATUS              RESTARTS   AGE
hydraa-pod-e8b66aa5-57ee-4edb-9986-9120a9829f98   0/3     ContainerCreating   0          0s
hydraa-pod-e8b66aa5-57ee-4edb-9986-9120a9829f98   0/3     ContainerCreating   0          0s
hydraa-pod-e8b66aa5-57ee-4edb-9986-9120a9829f98   1/3     NotReady            0          12s
hydraa-pod-e8b66aa5-57ee-4edb-9986-9120a9829f98   0/3     Completed           0          13s
hydraa-pod-e8b66aa5-57ee-4edb-9986-9120a9829f98   0/3     Completed           0          14s
hydraa-pod-e8b66aa5-57ee-4edb-9986-9120a9829f98   0/3     Completed           0          15s
```

```python
In [4]: caas_mgr.shutdown()
deleting ssh keys
deleting key-name from cloud storage
deleteing server fd5814d3-b95b-44ff-a938-0e6b012c08aa
deleting allocated ip
```

