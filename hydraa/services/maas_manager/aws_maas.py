import time
import threading as mt

from datetime import datetime, timedelta

class AwsMaas(mt.Thread):

    def __init__(self, cloud_watch_client):
        
        super(AwsMaas, self).__init__(args=(cloud_watch_client))
        self.cw_client   = cloud_watch_client
        self._stop_event = mt.Event()
    
    def start(self, InstanceId):

        try:
            ec2_cpu_thrd = mt.Thread(name='AwsMaasCpu', target=self.moniter_ec2_cpu, kwargs = {'InstanceId' : InstanceId})
            ec2_cpu_thrd.daemon = True
            ec2_cpu_thrd.start()


        except Exception as e:
            raise Exception(e)
    

    def moniter_ec2_cpu(self, InstanceId):
        
        print('monitering {0} started...'.format(InstanceId))
        while not self._stop_event.is_set():
            response = self.cw_client.get_metric_statistics(Namespace='AWS/EC2',
                                                    MetricName='CPUUtilization',
                                                    Dimensions=[{'Name': 'InstanceId',
                                                                'Value':  InstanceId},],
                                                    StartTime=datetime.now() - timedelta(days=1),
                                                    EndTime=datetime.now(),
                                                    Period=86400,
                                                    Statistics=['Average',],
                                                    Unit='Percent')

            for k, v in response.items():
                if k == 'Datapoints':
                    for y in v:
                        print(y['Average'])
            

            # this will controll the number of requests
            # per second. More requests = More money
            time.sleep(1)
        print('monitering stopped...')
    

    def moniter_ec2_mem(self):
        raise NotImplementedError
    

    # --------------------------------------------------------------------------
    #
    def stop(self):
        self._stop_event.set()
        
  