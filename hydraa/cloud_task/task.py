from typing import OrderedDict

class Task(object):
    """
    Base class for a cloud Task instance.
    """
    def __init__(self):
        pass

    def name(self):
        pass

    def id(self):
        pass

    def run_id(self):
        """represents the run id that the task belongs to"""
        pass


    def provider(self):
        """represents the ptovider of that task (AZURE, AWS, GCLOUD)"""
        pass

    def type(self):
        """represents the Container of function task"""
        pass

    
    def ip(self):
        pass

    
    def dns(self):
        pass

    
    def retry(self):
        pass

    
    def retry_number(self) -> bool:
        pass

    
    def memory(self) -> int:
        pass


    def cores(self) -> float:
        pass

    
    def status(self) -> str:
        pass

    
    def events(self) -> OrderedDict:
        pass


    def cmd(self) -> list:
        pass


    def is_service(self):
        """True if task needs to run for every.
           if True then self.retry() must be True
        """
        pass
