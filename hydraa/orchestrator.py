import time
import queue
import atexit
import networkx as nx
import threading       as mt
import multiprocessing as mp

from multiprocessing import Manager
from hydraa.cloud_task.task import Task



Waiting  = 'waiting'
Running  = 'running'
Finished = 'finished'

HPC      = 'hpc'
CLOUD    = 'cloud'

class HybridWorkflow:
    def __init__(self, cloud_manager=None, hpc_manager=None) -> None:

        if not cloud_manager and not hpc_manager:
            raise Exception('The workflow component must be initialized with at least one manager')
        
        self.hpc_manager   = hpc_manager
        self.cloud_manager = cloud_manager

        self.workflow = nx.DiGraph()
        # use mp.Queue instances to proxy tasks to the worker processes
        self.work_queue    = mp.Queue()
        self.work2_queue   = mp.Queue()
        self._task_counter = 0

        manager = Manager()
        self.waiting  = manager.dict()
        self.running  = manager.dict()
        self.finished = manager.dict()

        self.submission_lock = mt.Lock()

        # start threads to feed / drain the workers
        self.stop_event  = mt.Event()
        self.get_works   = mt.Thread(target=self.get_work, name="GatWorkFunction")
        self.distributer = mt.Thread(target=self.distribute, name="SendWorkFunction")

        self.get_works.daemon   = True
        self.distributer.daemon = True

        self.get_works.start()
        self.distributer.start()

        atexit.register(self.stop_background, self.stop_event, [self.get_works, self.distributer])
    

    def add_task(self, task: Task):
        tid = self._task_counter
        task.id = tid
        self.workflow.add_node(tid, tid=tid, task=task)
        self._task_counter +=1
        return True


    def remove_task(self, task_id):
        self.workflow.remove_node(task_id)
        self._task_counter -=1
        return True


    def add_cloud_dependency(self, cloud_dependee: Task, hpc_depnder: Task):

        # check that both tasks are already in the DAG to prevent
        # duplications
        try:
            dependee = self.workflow.nodes[cloud_dependee.id]
            depender = self.workflow.nodes[hpc_depnder.id]
            self.workflow.add_edge(cloud_dependee.id, hpc_depnder.id)

        except KeyError:
            raise Exception('cloud or hpc task is not in the DAG')

        # check if this dependcy won't create a cycle!
        if not nx.is_directed_acyclic_graph(self.workflow):
            self.workflow.remove_edge(cloud_dependee.id, hpc_depnder.id)
            raise Exception('adding cloud dependecy will create infinite cycle')
        
        hpc_depnder.arch    = HPC
        cloud_dependee.arch = CLOUD

        return True


    def add_hpc_dependency(self, hpc_dependee: Task, cloud_depnder: Task):

        try:
            dependee = self.workflow.nodes[hpc_dependee.id]
            depender = self.workflow.nodes[cloud_depnder.id]
            self.workflow.add_edge(hpc_dependee.id, cloud_depnder.id)
        
        except KeyError:
            raise Exception('cloud or hpc task is not in the DAG')

        if not nx.is_directed_acyclic_graph(self.workflow):
            self.workflow.remove_edge(hpc_dependee.id, cloud_depnder.id)
            raise Exception('Adding hpc dependecy will create infinite cycle')
        
        hpc_dependee.arch  = HPC
        cloud_depnder.arch = CLOUD

        return True
    

    def remove_dependency(self, dependee, depnder):
        self.workflow.remove_edge(dependee,depnder)
        return True

    # stop the background task gracefully before exit
    def stop_background(self, stop_event, threads):
        # request the background thread stop
        stop_event.set()
        # wait for the background thread to stop
        for thread in threads:
            thread.join()
    

    def size(self):
        return self.workflow.size()



    def get_work(self):
        '''
        thread feeding tasks pulled from the ZMQ work queue to worker processes
        '''
        # FIXME: This drains the qork queue with no regard of load balancing.
        #        For example, the first <n_cores> tasks may stall this executer
        #        for a long time, but new tasks are pulled nonetheless, even if
        #        other executors are not stalling and could execute them timely.
        #        We should at most fill a cache of limited size.

        while True:
            try:
                task = self.work2_queue.get(block=True, timeout=0.1)
            except queue.Empty:
                continue
            if task:
                # send task individually to load balance workers
                self.work_queue.put(task)
    

    def task_callbacks(self, task):
        """
        a thread to check on the task done/failed callback
        all the time **this is just an EXAMPLE
        """
        while not self.stop_event:
            if task.done:
                self.submission_lock.acquire()
                self.running.pop(task['tid'])
                self.finished[task['tid']] = Finished
                self.submission_lock.release()
            if task.failed:
                pass

            elif task.wait:
                pass
            else:
                pass

    def distribute(self):
        while True:
            try:
                task = self.work_queue.get(block=True, timeout=0.1)
                if task:
                    with self.submission_lock:
                        task_input = self.finished.get(task['tid'])
                        if task['task'].arch == HPC:
                            print('task {0} sent to HPC manager'.format(task['tid']))

                            if task_input:
                                result = task['task'].cmd(*tuple(task_input))
                            else:
                                result = task['task'].cmd()
    
                            #self.hpc_manager.submit(task)
                        if task['task'].arch == CLOUD:
                            print('task {0} sent to CLOUD manager'.format(task['tid']))
                            if task_input:
                                result = task['task'].cmd(*tuple(task_input))
                            else:
                                result = task['task'].cmd()
                            
                            #self.cloud_manager.submit(task)
                        while True:
                            if result:
                                #self.submission_lock.acquire()
                                self.running.pop(task['tid'])
                                task['task'].result = result
                                print(task['task'].result)
                                self.finished[task['tid']] = result
                                #self.submission_lock.release()
                                break
                            else:
                                time.sleep(1)
                    
            except queue.Empty:
                continue
            print('Running tasks from execute: {0}'.format(self.running))
            print('Finished Tasks from execute: {0}'.format(self.finished))


    def orchestrate(self):
        while True:
            counter = 0
            # iterate on the sorted DAG
            for node in nx.topological_sort(self.workflow):

                # get each not dependecies via predecessor
                node_obj = self.workflow.nodes[node]
                node_dep = list(self.workflow.predecessors(node))

                # (1) check if the node is finished
                if node in self.finished:
                    continue
    
                elif node in self.running:
                    continue

                # (1) check if the node is waiting to be executed
                elif node in self.waiting:
                    # (3) if all dependecies of this node is in finished then send it to execution
                    if(all(key in self.finished.keys() for key in node_dep)):
                        node_input = tuple(self.finished[key] for key in node_dep)
                        print(node_input)
                        self.finished[node_obj['tid']] = node_input

                        print('task {0} was in wait_list, all dep. solved, sending to execute'.format(node))
                        # mark this task as running
                        self.running[node] = Running

                        # remove this task from waiting
                        self.waiting.pop(node)

                        # send this task to distribuation
                        self.work2_queue.put(node_obj)

                        # increase the internal running tasks counters by 1
                        counter = counter +1

                    # (4) if not then we can not do anything beside waiting
                    else:
                        continue

                # (5) if this node has depndencies
                if node_dep:
                    # if all of the dep. in task_finished then execute it
                    if(all(key in self.finished.keys() for key in node_dep)):
                        print('task {0} will be sent to execute because dep {1} is finished {2}'.format(node, node_dep, self.finished))
                        
                        node_input = tuple(self.finished[key] for key in node_dep)
                        print(node_input)
                        
                        self.finished[node_obj['tid']] = node_input

                        self.running[node] = Running
                        self.work2_queue.put(node_obj)
                        counter = counter +1
                        continue
                    
                    elif (all(key in self.running.keys() for key in node_dep)):
                        print('task {0} will be skipped as all dep {1} are running'.format(node, node_dep))
                        if node not in self.waiting:
                            self.waiting[node] = Waiting
                        continue

                    # all of the dependecies has other depndecies so just skip it and wait
                    elif (all(key in node_dep for key in self.waiting.keys())):
                        continue

                # if this node has no predecssors then we can execute it
                else:
                    print('task {0} will be sent to execute because it has no dep'.format(node))
                    self.running[node] = Running
                    self.work2_queue.put(node_obj)
                    counter = counter +1

            if self.workflow.size() == counter:
                break