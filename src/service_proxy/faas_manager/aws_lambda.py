from src.service_proxy.cost_manager.aws_cost import AwsCost
"""
"""
__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'
class AwsFaas(AwsCost):
    def __init__(self):
        pass
    
    def run_aws_function(self):
        raise NotImplementedError