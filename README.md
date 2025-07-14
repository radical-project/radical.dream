
# Hydraa: Multi-Resource Task Orchestration (Project Dream)

Hydraa is a core component of **Project Dream**, providing seamless and concurrent execution of heterogeneous computational tasks such as containers, executables, and Python functions across **multiple cloud providers** (both inter- and cross-provider), **HPC machines** and **clusters**.  

---

## Supported Cloud Platforms

- **Azure**
  - Azure Container Services (ACS)
  - Azure Kubernetes Service (AKS)
- **AWS**
  - Elastic Container Service (ECS) (Fargate & EC2)
  - Elastic Kubernetes Service (EKS)
- **OpenStack & Native Kubernetes**
  - OpenStack clusters (e.g., JET2, CHI)
  - Any multi-node Kubernetes deployment


## Supported HPC Machines

Please refer to RADICAL-Pilot (RP) supported systems as, Project Dream leverages the underlying capabilotes of RP to execute tasks on HPC here: [supported HPC Machines](https://radicalpilot.readthedocs.io/en/stable/supported.html)

---

## Getting Started

Hydraa abstracts away the complexity of provisioning and orchestration. The typical flow includes:

### 1️⃣ Define and provision resources
Use Hydraa to define VMs and containers across multiple HPC and clouds. Examples include:

- AWS Fargate & EC2 instances
- Azure ACS instances
- OpenStack KVM nodes
- GPUs and CPUs nodes on HPC machines.

See the examples section for how to define and launch them.

---

### 2️⃣ Submit Workloads
Hydraa supports **batch** and **streaming** task submission to the cloud resources you created:

- Batch: Submit hundreds or thousands of tasks across AWS and Azure.
- Stream: Submit tasks incrementally as they are created.

Each task supports resource constraints (vCPUs, memory), custom container images, and commands.

---

### 3️⃣ Monitor & Control Execution
Tasks are future-based and integrate naturally into custom Python logic. You can wait for results, chain dependent tasks, or implement your own control flow.

---

## Advanced Capabilities

### ✅ Heterogeneous Workloads
Hydraa can execute complex MPI jobs, Kubernetes-native workloads, and integrate with tools like Kubeflow for distributed training.

### ✅ Workflow Engines Integration
Hydraa integrates with workflow engines such as Argo. You can define container workflows with task dependencies, shared persistent volumes (PVCs), and scalable pipelines — all submitted to Kubernetes clusters.

---

## Examples

For full working code examples, refer to the following:

- [Provisioning multi-cloud VMs](#)
- [Batch & stream task submission](#)
- [MPI job orchestration with Kubeflow](#)
- [Workflow definition and Argo integration](#)

Example snippets include:

```python
from hydraa.cloud_vm import vm
from hydraa import providers, services
from hydraa.cloud_task.task import Task
```

```python
# Example: Submit a batch of tasks
tasks = [Task(vcpus=1, memory=7, provider=AWS, image="noop", cmd=["echo", "hello"]) for _ in range(100)]
caas_mgr.submit(tasks)
```

```python
# Example: Define and run an MPI job with Kubeflow
from hydraa.services.caas_manager.integrations.kubeflow import KubeflowMPILauncher
mpi_launcher = KubeflowMPILauncher(caas_mgr.Jet2Caas)
mpi_launcher.launch(mpi_tasks, num_workers=1, slots_per_worker=5)
```

```python
# Example: Create an Argo workflow
from hydraa.services.caas_manager.integrations.workflows import ContainerSetWorkflow
wf = ContainerSetWorkflow(name="my-workflow", manager=caas_mgr.Jet2Caas, volume=pvc)
wf.add_tasks([task1, task2])
wf.run()
```

---

## Why Hydraa?

Hydraa makes it easy to:

- Provision and scale compute on multiple HPC and cloud providers at once.
- Submit and manage tasks with simple Python APIs.
- Orchestrate complex, dependent workflows at scale.
- Support heterogeneous clusters and container technologies.

---
