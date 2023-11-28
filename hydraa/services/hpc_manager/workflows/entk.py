import radical.entk as re
from ..manager import HpcManager

class EntkPipeline:
    def __init__(self, manager):

        self.pipelines = []
        if not isinstance(manager, HpcManager):
            raise TypeError("PipelineWorkflow requires a HpcManager.")
        self.tasks = []
        self.manager = manager


    # --------------------------------------------------------------------------
    #
    def add_tasks(self, tasks) -> None:
        """
        Add tasks to the workflow.

        Parameters
        ----------
        tasks : list
            A list of Task objects to be added to the workflow.
        """
        for task in tasks:
            self.tasks.append(task)
    
    def _map_hydraa_to_entk_task(self, task):

        entk_task = re.Task()
        entk_task.name = task.name
        entk_task.arguments = task.args
        entk_task.executable = task.cmd
        entk_task.mem_per_process = task.memory
        entk_task.cpu_reqs.cpu_processes = task.vcpus
        entk_task.gpu_reqs.gpu_processes = 0

        return entk_task

    # --------------------------------------------------------------------------
    #
    def create(self) -> str:

        all_stages = []
        pipeline = re.Pipeline()
        # All tasks that can run immediately without dependencies
        universal_stage = re.Stage()

        for task in self.tasks:
            task.id = str(self.manager._task_id)
            task.name = 'ctask.{0}'.format(self.manager._task_id)
            entk_task = self._map_hydraa_to_entk_task(task)
            print(entk_task)

            if task.inputs:
                entk_task.upload_input_data = task.inputs

            if task.get_dependency():
                stage = re.Stage()
                stage.add_tasks(entk_task)
                task.stage = stage
                all_stages.append(stage)
            else:
                universal_stage.add_tasks(entk_task)
                task.stage = universal_stage

            if task.outputs:
                to_be_linked = []
                for output in task.outputs:
                    to_be_linked.append([f'$Pipeline_{pipeline.name}_Stage_{task.stage.name}_Task_{task.name}/{output}']) 
                entk_task.link_output_data = to_be_linked
        
            self.manager._task_id +=1

        all_stages.append(universal_stage)
        pipeline.add_stages(all_stages)

        self.pipelines.append(pipeline)


    def run(self) -> None:
        """
        Run the workflow.
        """
        if not self.pipelines:
            raise ValueError("No pipelines to run. Please create a pipeline first.")

        return self.manager.submit(backend='entk', work=self.pipelines)
