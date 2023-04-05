import os
import uuid
import radical.pilot as rp
import radical.utils as ru


class RadicalPilot():
    def __init__(self, pdesc: rp.PilotDescription, tasks) -> None:

        self.pdesc = pdesc
        self.tasks = tasks
        self.run_id = str(uuid.uuid4())
        self.check_pre_start()
    

    def start(self):
        print('starting run HPC-RP.{0}'.format(self.run_id))
        self.session = rp.Session()
        self.pmgr   = rp.PilotManager(session=self.session)
        self.tmgr   = rp.TaskManager(session=self.session)
        pilot = self.pmgr.submit_pilots(self.pdesc)

        # Register the pilot in a TaskManager object.
        self.tmgr.add_pilots(pilot)

        self.submit(self.tasks)


    def check_pre_start(self):

        try:
            os.environ["RADICAL_PILOT_DBURL"]
        except KeyError:
            print("Please set RADICAL_PILOT_DBURL?")
        
        os.environ.get("RADICAL_PROFILE", "True")
        os.environ.get("RADICAL_LOG_LVL", "DEBUG")


    def submit(self, tasks):
        # Create a new session. No need to try/except this: if session creation
        # fails, there is not much we can do anyways...
        try:
            tds = list()

            for task in tasks:
                # create a new task description, and fill it.
                td = rp.TaskDescription()
                td.executable     = task.cmd
                td.ranks          = task.vcpus
                td.memory         = task.memory

                tds.append(td)

            # Submit the previously created task descriptions to the
            # PilotManager. This will trigger the selected scheduler to start
            # assigning tasks to the pilots.
            self.tmgr.submit_tasks(tds)
            self.tmgr.wait_tasks()

        except Exception as e:
            ru.print_exception_trace()
            raise

        except (KeyboardInterrupt, SystemExit):
            ru.print_exception_trace()
        
        finally:
            self.session.close(download=True)
