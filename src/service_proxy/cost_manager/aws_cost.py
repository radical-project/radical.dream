import re
import ast
import datetime
from dateutil.tz import *
from boto3.dynamodb.conditions import Key, Attr
"""
AwsCost object contains the current pricings of the AWS cloud services.
AWSCost offers the capabilities to obtain the cost of:
* running a container ECS task
* running a container EC2 task
* running a lambda function task
https://github.com/aws-samples/amazon-ecs-chargeback/blob/master/ecs-chargeback
"""

# cpu to memory weight
CTMW    = 0.5
HOURLY  = 'hour'
DAILY   = 'day'
MONTHLY = 'month'
YEARLY  = 'year'

_PRICINGS = {}
_REGRIONS = {
            "us-east-2"         : "US East (Ohio)",
            "us-east-1"         : "US East (N. Virginia)",
            "us-west-1"         : "US West (N. California)",
            "us-west-2"         : "US West (Oregon)",
            "ap-northeast-1"    : "Asia Pacific (Tokyo)",
            "ap-northeast-2"    : "Asia Pacific (Seoul)",
            "ap-northeast-3"    : "Asia Pacific (Osaka-Local)",
            "ap-south-1"        : "Asia Pacific (Mumbai)",
            "ap-southeast-1"    : "Asia Pacific (Singapore)",
            "ap-southeast-2"    : "Asia Pacific (Sydney)",
            "ca-central-1"      : "Canada (Central)",
            "cn-north-1"        : "China (Beijing)",
            "cn-northwest-1"    : "China (Ningxia)",
            "eu-central-1"      : "EU (Frankfurt)",
            "eu-west-1"         : "EU (Ireland)",
            "eu-west-2"         : "EU (London)",
            "eu-west-3"         : "EU (Paris)",
            "sa-east-1"         : "South America (São Paulo)",
            "us-gov-west-1"     : "AWS GovCloud (US)"}


# --------------------------------------------------------------------------
#
class AwsCost:

    # --------------------------------------------------------------------------
    #
    def __init__(self, prc_client, dydb_resource, region_name,
                         cluster_name=None, service_name=None,
                         ecs_client=None, ec2_client=None):
        
        self._prc_client    = prc_client
        self._dydb_resource = dydb_resource
        self._ecs_client    = ecs_client
        self._ec2_client    = ec2_client

        self._cluster_name = cluster_name
        self._service_name = service_name
        self._region_name  = region_name
    
    
    # --------------------------------------------------------------------------
    #
    def get_cost(self, launch_type, batch_size, runTime, cpu=0, mem=0):

        fgate_total_cost = []

        if launch_type == 'EC2':
            instance_cost = self.ec2_pricing(self._region_name, 't2.micro', 'Linux', runTime)
            return instance_cost
        
        if launch_type == 'FARGATE':
            if not cpu and not mem:
                raise Exception('cpu and memory is required to calaculte Fargate price')

            task_cost = self.cost_of_fgtask(self._region_name, batch_size, cpu, mem, 'Linux', runTime)
            fgate_total_cost.append(task_cost)

            return sum(fgate_total_cost)



    # --------------------------------------------------------------------------
    #
    def ec2_pricing(self, region, instance_type, ostype, time):
            """
            Query AWS Pricing APIs to find cost of EC2 instance in the region.
            Given the paramters we use at input, we should get a UNIQUE result.
            TODO: In the current version, we only consider OnDemand price. If
            we start considering actual cost, we need to consider input from 
            CUR on an hourly basis.
            """
            svc_code = 'AmazonEC2'
            response = self._prc_client.get_products(ServiceCode=svc_code,
                Filters = [
                    {'Type' :'TERM_MATCH', 'Field':'location',          'Value': _REGRIONS[region]},
                    {'Type' :'TERM_MATCH', 'Field': 'servicecode',      'Value': svc_code},
                    {'Type' :'TERM_MATCH', 'Field':'instanceType',      'Value': instance_type},
                    {'Type' :'TERM_MATCH', 'Field': 'operatingSystem',  'Value': ostype}
                ],
                MaxResults=100
            )

            ret_list = []
            if 'PriceList' in response:
                for iter in response['PriceList']:
                    ret_dict = {}
                    mydict = ast.literal_eval(iter)
                    ret_dict['memory'] = mydict['product']['attributes']['memory']
                    ret_dict['vcpu'] = mydict['product']['attributes']['vcpu']
                    ret_dict['instanceType'] = mydict['product']['attributes']['instanceType']
                    ret_dict['operatingSystem'] = mydict['product']['attributes']['operatingSystem']
                    ret_dict['normalizationSizeFactor'] = mydict['product']['attributes']['normalizationSizeFactor']

                    mydict_terms = mydict['terms']['OnDemand'][ list( mydict['terms']['OnDemand'].keys() )[0]]
                    ret_dict['unit'] = mydict_terms['priceDimensions'][list( mydict_terms['priceDimensions'].keys() )[0]]['unit']
                    ret_dict['pricePerUnit'] = mydict_terms['priceDimensions'][list( mydict_terms['priceDimensions'].keys() )[0]]['pricePerUnit']
                    ret_list.append(ret_dict)

            ec2_cpu  = float( ret_list[0]['vcpu'] )
            ec2_mem  = float( re.findall("[+-]?\d+\.?\d*", ret_list[0]['memory'])[0] )
            ec2_cost = float( ret_list[0]['pricePerUnit']['USD'] )
            #return(ec2_cpu, ec2_mem, ec2_cost)

            cost = ec2_cost * (time / 60)

            return cost


    # --------------------------------------------------------------------------
    #
    def ecs_pricing(self, region):
        """
        Get Fargate Pricing in the region.
        """
        svc_code = 'AmazonECS'

        response = self._prc_client.get_products(ServiceCode=svc_code, 
            Filters = [
                {'Type' :'TERM_MATCH', 'Field':'location',          'Value': region},
                {'Type' :'TERM_MATCH', 'Field': 'servicecode',      'Value': svc_code},
            ],
            MaxResults=100
        )

        cpu_cost = 0.0
        mem_cost = 0.0

        if 'PriceList' in response:
            for iter in response['PriceList']:
                mydict = ast.literal_eval(iter)
                mydict_terms = mydict['terms']['OnDemand'][list( mydict['terms']['OnDemand'].keys() )[0]]
                mydict_price_dim = mydict_terms['priceDimensions'][list( mydict_terms['priceDimensions'].keys() )[0]]
                if mydict_price_dim['description'].find('CPU') > -1:
                    cpu_cost = mydict_price_dim['pricePerUnit']['USD']
                if mydict_price_dim['description'].find('Memory') > -1:
                    mem_cost = mydict_price_dim['pricePerUnit']['USD']

        return(cpu_cost, mem_cost)


    # --------------------------------------------------------------------------
    #
    def duration(self, startedAt, stoppedAt, startMeter, stopMeter, runTime, now):
        """
        Get the duration for which the task's cost needs to be calculated.
        This will vary depending on the CLI's input parameter (task lifetime,
        particular month, last N days etc.) and how long the task has run.
        """
        mRunTime = 0.0
        task_start = datetime.datetime.strptime(startedAt, '%Y-%m-%dT%H:%M:%S.%fZ')
        task_start = task_start.replace(tzinfo=datetime.timezone.utc)

        if (stoppedAt == 'STILL-RUNNING'):
            task_stop = now
        else:
            task_stop = datetime.datetime.strptime(stoppedAt, '%Y-%m-%dT%H:%M:%S.%fZ')
            task_stop = task_stop.replace(tzinfo=datetime.timezone.utc)

        # Return the complete task lifetime in seconds if metering duration is not provided at input.
        if not startMeter or not stopMeter:
            mRunTime = round ( (task_stop - task_start).total_seconds() )
            print('In duration (task lifetime): mRunTime=%f',  mRunTime)
            return(mRunTime)

        # Task runtime:              |------------|
        # Metering duration: |----|     or            |----|
        if (task_start >= stopMeter) or (task_stop <= startMeter): 
            mRunTime = 0.0
            print('In duration (meter duration different OOB): mRunTime=%f',  mRunTime)
            return(mRunTime)

        # Remaining scenarios:
        #
        # Task runtime:                |-------------|
        # Metering duration:   |----------|  or   |------|
        # Calculated duration:         |--|  or   |--|
        #
        # Task runtime:                |-------------|
        # Metering duration:              |-------|
        # Calculated duration:            |-------|
        #
        # Task runtime:                |-------------|
        # Metering duration:   |-------------------------|
        # Calculated duration:         |-------------|
        #

        calc_start = startMeter if (startMeter >= task_start) else task_start
        calc_stop = task_stop if (stopMeter >= task_stop) else stopMeter

        mRunTime = round ( (calc_stop - calc_start).total_seconds() )
        print('In duration(), mRunTime = %f', mRunTime)
        return(mRunTime)


    # --------------------------------------------------------------------------
    #
    def get_datetime_start_end(now, month, days, hours):

        logging.debug('In get_datetime_start_end(). month = %s, days = %s, hours = %s', month, days, hours)
        meter_end = now

        if month:
            # Will accept MM/YY and MM/YYYY format as input.
            regex = r"(?<![/\d])(?:0\d|[1][012])/(?:19|20)?\d{2}(?![/\d])"
            r = re.match(regex, month)
            if not r:
                raise("Month provided doesn't look valid: %s" % (month))
            [m,y] = r.group().split('/')
            iy = 2000 + int(y) if int(y) <= 99 else int(y)
            im = int(m)

            meter_start = datetime.datetime(iy, im, 1, 0, 0, 0, 0, tzinfo=tzutc())
            meter_end = meter_start + relativedelta(months=1)

        if days:
            # Last N days = datetime(now) - timedelta (days = N)
            # Last N days could also be last N compelted days.
            # We use the former approach.
            if not days.isdigit():
                print("Duration provided is not a integer: %s" % (days))
                raise
            meter_start = meter_end - datetime.timedelta(days = int(days))
        if hours:
            if not hours.isdigit():
                print("Duration provided is not a integer" % (hours))
                raise
            meter_start = meter_end - datetime.timedelta(hours = int(hours))

        return (meter_start, meter_end)


    # --------------------------------------------------------------------------
    #
    def cost_of_fgtask(self, region, batch_size, cpu, memory, ostype, runTime):

        pricing_key = 'fargate_' + region
        if pricing_key not in _PRICINGS:
            # First time. Updating Dictionary
            # Workarond - for DUBLIN (cpu_cost, mem_cost) = ecs_pricing(region, session)
            #(cpu_cost, mem_cost) = ecs_pricing(_REGRIONS[region], session)
            (cpu_cost, mem_cost) = self.ecs_pricing(_REGRIONS[region])
            _PRICINGS[pricing_key]={}
            _PRICINGS[pricing_key]['cpu']    = cpu_cost
            _PRICINGS[pricing_key]['memory'] = mem_cost

        mem_charges = ( (float(memory)) / 1024.0 ) * float(_PRICINGS[pricing_key]['memory']) * (runTime/60.0/60.0)
        cpu_charges = ( (float(cpu)) / 1024.0 )    * float(_PRICINGS[pricing_key]['cpu'])    * (runTime/60.0/60.0)

        #print('In cost_of_fgtask: mem_charges=%f, cpu_charges=%f',  mem_charges, cpu_charges)
        return (mem_charges + cpu_charges) * batch_size


    # --------------------------------------------------------------------------
    #
    def cost_of_ec2task(self, region, cpu, memory, ostype, instanceType, runTime):
        """
        Get Cost in USD to run a ECS task where launchMode==EC2.
        The AWS Pricing API returns all costs in hours. runTime is in seconds.
        """
        global _PRICINGS
        global _REGRIONS

        pricing_key = '_'.join(['ec2',region, instanceType, ostype]) 
        if pricing_key not in _PRICINGS:
            # Workaround for DUBLIN, Shared Tenancy and Linux
            (ec2_cpu, ec2_mem, ec2_cost) = self.ec2_pricing(_REGRIONS[region], instanceType, 'Shared', 'Linux')
            _PRICINGS[pricing_key]={}
            _PRICINGS[pricing_key]['cpu']    = ec2_cpu   # Number of CPUs on the EC2 instance
            _PRICINGS[pricing_key]['memory'] = ec2_mem   # GiB of memory on the EC2 instance
            _PRICINGS[pricing_key]['cost']   = ec2_cost  # Cost of EC2 instance (On-demand)

        # Corner case: When no CPU is assigned to a ECS Task, cpushares = 0
        # Workaround: Assume a minimum cpushare, say 128 or 256 (0.25 vcpu is the minimum on Fargate).
        if cpu == '0':
            cpu = '128'

        # Split EC2 cost bewtween memory and weights
        ec2_cpu2mem = ec2_cpu2mem_weights(_PRICINGS[pricing_key]['memory'], _PRICINGS[pricing_key]['cpu'])
        cpu_charges = ((float(cpu)) / 1024.0 / _PRICINGS[pricing_key]['cpu']) * ( float(_PRICINGS[pricing_key]['cost']) * ec2_cpu2mem ) * (runTime/60.0/60.0)
        mem_charges = ((float(memory)) / 1024.0 / _PRICINGS[pricing_key]['memory'] ) * ( float(_PRICINGS[pricing_key]['cost']) * (1.0 - ec2_cpu2mem) ) * (runTime/60.0/60.0)

        print('In cost_of_ec2task: mem_charges=%f, cpu_charges=%f',  mem_charges, cpu_charges)
        return(mem_charges, cpu_charges)


    # --------------------------------------------------------------------------
    #
    def cost_of_service(self, tasks, meter_start, meter_end, now):
        fargate_service_cpu_cost = 0.0
        fargate_service_mem_cost = 0.0
        ec2_service_cpu_cost = 0.0
        ec2_service_mem_cost = 0.0

        if 'Items' in tasks:
            for task in tasks['Items']:
                runTime = duration(task['startedAt'], task['stoppedAt'], meter_start, meter_end, float(task['runTime']), now)

                logging.debug("In cost_of_service: runTime = %f seconds", runTime)
                if task['launchType'] == 'FARGATE':
                    fargate_mem_charges,fargate_cpu_charges = cost_of_fgtask(task['region'], task['cpu'], task['memory'], task['osType'], runTime)
                    fargate_service_mem_cost += fargate_mem_charges
                    fargate_service_cpu_cost += fargate_cpu_charges
                else:
                    # EC2 Task
                    ec2_mem_charges, ec2_cpu_charges = cost_of_ec2task(task['region'], task['cpu'], task['memory'], task['osType'], task['instanceType'], runTime)
                    ec2_service_mem_cost += ec2_mem_charges
                    ec2_service_cpu_cost += ec2_cpu_charges

        return(fargate_service_cpu_cost, fargate_service_mem_cost, ec2_service_mem_cost, ec2_service_cpu_cost)


    # --------------------------------------------------------------------------
    #
    def fetch_table(self, table, region, cluster, service):
        """
        Scan the DynamoDB table to get all tasks in a service.
        Input - region, ECS ClusterARN and ECS ServiceName
        """

        resp = table.scan(FilterExpression=Attr('group').eq('service') &
                        Attr('groupName').eq(service) &
                        Attr('region').eq(region) &
                        Attr('clusterArn').eq(cluster))
        return (resp)


    # --------------------------------------------------------------------------
    #
    def get_cost_report(self, period):

        """
        """
        now = datetime.datetime.now(tz=tzutc())

        table = self._dydb_resource.Table("ECSTaskStatus")
        tasks = self.fetch_table(table, self._region_name, self._cluster_name, self._service_name)

        if metered_results:
            (meter_start, meter_end) = get_datetime_start_end(now, MONTHLY, DAILY, HOURLY)
            (fg_cpu, fg_mem, ec2_mem, ec2_cpu) = cost_of_service(tasks, meter_start, meter_end, now)
        else: 
            (fg_cpu, fg_mem, ec2_mem, ec2_cpu) = cost_of_service(tasks, 0, 0, now)


        print("Main: fg_cpu=%f, fg_mem=%f, ec2_mem=%f, ec2_cpu=%f", fg_cpu, fg_mem, ec2_mem, ec2_cpu)

        print("#####################################################################")
        print("#")
        print("# ECS Region  : %s, ECS Service Name: %s" % (region, service) )
        print("# ECS Cluster : %s" % (cluster))
        print("#")

        if metered_results:
            if period == MONTHLY:
                print("# Cost calculated for month %s" % (MONTHLY) )
            else:
                print("# Cost calculated for last %s %s" % (DAILY if period == DAILY else HOURLY,
                                                            DAILY if period == DAILY else HOURLY))

        print("#")

        if ec2_mem or ec2_cpu:
            print("# Amazon ECS Service Cost           : %.6f USD" % (ec2_mem+ec2_cpu) )
            print("#         (Launch Type : EC2)")
            print("#         EC2 vCPU Usage Cost       : %.6f USD" % (ec2_cpu) )
            print("#         EC2 Memory Usage Cost     : %.6f USD" % (ec2_mem) )
        if fg_cpu or fg_mem:
            print("# Amazon ECS Service Cost           : %.6f USD" % (fg_mem+fg_cpu) )
            print("#         (Launch Type : FARGATE)")
            print("#         Fargate vCPU Usage Cost   : %.6f USD" % (fg_cpu) )
            print("#         Fargate Memory Usage Cost : %.6f USD" % (fg_mem) )
            print("#")

        if not (fg_cpu or fg_mem or ec2_mem or ec2_cpu):
            print("# Service Cost: 0 USD. Service not running in specified duration!")
            print("#")

        print("#####################################################################")
