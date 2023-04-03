import radical.pilot as rp
import radical.utils as ru


class RadicalPilot():
	def __init__(self, pdesc: rp.PilotDescription) -> None:

		self.pdesc = pdesc


	def start(self, tasks):
        # Create a new session. No need to try/except this: if session creation
        # fails, there is not much we can do anyways...
		try:
			session = rp.Session()
			pmgr   = rp.PilotManager(session=session)
			tmgr   = rp.TaskManager(session=session)
			pilot = pmgr.submit_pilots(self.pdesc)

			# Register the pilot in a TaskManager object.
			tmgr.add_pilots(pilot)

			tds = list()

			for task in range(tasks):
				# create a new task description, and fill it.
				td = rp.TaskDescription()
				td.executable     = task.cmd
				td.ranks          = task.vcpus
				td.memory         = task.memory
				
				tds.append(td)

			# Submit the previously created task descriptions to the
			# PilotManager. This will trigger the selected scheduler to start
			# assigning tasks to the pilots.
			tmgr.submit_tasks(tds)
			tmgr.wait_tasks()

		except Exception as e:
			ru.print_exception_trace()
			raise

		except (KeyboardInterrupt, SystemExit):
			ru.print_exception_trace()
		
		finally:
			session.close(download=True)