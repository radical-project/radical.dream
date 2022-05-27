from datetime import datetime, timedelta

class AwsMaas:

    def __init__(self, cloud_watch_client):

        self.status = False
        self.cw_client = cloud_watch_client
    

    def moniter_ec2_cpu(self):
        
        response = self.cw_client.get_metric_statistics(Namespace='AWS/EC2',
                                                MetricName='CPUUtilization',
                                                Dimensions=[
                                                    {
                                                    'Name': 'InstanceId',
                                                    'Value': 'i-1234abcd'
                                                    },
                                                ],
                                                StartTime=datetime.now() - timedelta(seconds=600),
                                                EndTime=datetime.now(),
                                                Period=86400,
                                                Statistics=['Average',],
                                                Unit='Percent')
        
        for k, v in response.items():
            if k == 'Datapoints':
                for y in v:
                    print(y['Average'])
    

    def moniter_ec2_mem(self):
        raise NotImplementedError


  