from ..provider_proxy.proxy import proxy

class CaasManager(object):
    def __init__(self, proxy_mgr: proxy):
        if proxy:
            if isinstance(proxy_mgr, proxy):
                pass

    def get_container_cost(self):
        """
        calculate the cost of executing a 
        container on a provider
        """
        raise NotImplementedError

    def get_container_status(self, ID):
        """
        check if the contianer is still executing or 
        Done/failed
        """
        raise NotImplementedError
    
    def async_execute_container(self, container_path):
        """
        execute contianer and do not wait for it
        """
        raise NotImplementedError
    

    def sync_execute_container(self, container_path):
        """
        execute contianer and wait for it
        """
        raise NotImplementedError