
class service_manager():
    def __init__(self, managers=[]) -> None:
        self.managers = managers
    
    def start(self):
        for manager in self.managers:
            manager.start()
