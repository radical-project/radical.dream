class AwsCost:
    def __init(self, prc_client, ecs_client, ec2_client):
        
        self._prc_client = prc_client
        self._ecs_client = ecs_client
        self._ec2_client = ec2_client
    
    def ec2_pricing(self, region, instance_type, tenancy, ostype):
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
                    {'Type' :'TERM_MATCH', 'Field':'location',          'Value':region},
                    {'Type' :'TERM_MATCH', 'Field': 'servicecode',      'Value': svc_code},
                    {'Type' :'TERM_MATCH', 'Field': 'preInstalledSw',   'Value': 'NA'},
                    {'Type' :'TERM_MATCH', 'Field': 'tenancy',          'Value': tenancy},
                    {'Type' :'TERM_MATCH', 'Field':'instanceType',      'Value':instance_type},
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
            return(ec2_cpu, ec2_mem, ec2_cost)

    def ecs_pricing(self, region):
        """
        Get Fargate Pricing in the region.
        """
        svc_code = 'AmazonECS'

        response = self._prc_client.get_products(ServiceCode=svc_code, 
            Filters = [
                {'Type' :'TERM_MATCH', 'Field':'location',          'Value':region},
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
        

    def _duration(self, startedAt, stoppedAt, startMeter, stopMeter, runTime, now):
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
            logging.debug('In duration (task lifetime): mRunTime=%f',  mRunTime)
            return(mRunTime)

        # Task runtime:              |------------|
        # Metering duration: |----|     or            |----|
        if (task_start >= stopMeter) or (task_stop <= startMeter): 
            mRunTime = 0.0
            logging.debug('In duration (meter duration different OOB): mRunTime=%f',  mRunTime)
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
        logging.debug('In duration(), mRunTime = %f', mRunTime)
        return(mRunTime)

    def _cost_of_fgtask(self, region, cpu, memory, ostype, runTime, session):
        global pricing_dict
        global region_table

        pricing_key = 'fargate_' + region
        if pricing_key not in pricing_dict:
            # First time. Updating Dictionary
            # Workarond - for DUBLIN (cpu_cost, mem_cost) = ecs_pricing(region, session)
            (cpu_cost, mem_cost) = ecs_pricing(region_table[region], session)
            pricing_dict[pricing_key]={}
            pricing_dict[pricing_key]['cpu'] = cpu_cost
            pricing_dict[pricing_key]['memory'] = mem_cost

        mem_charges = ( (float(memory)) / 1024.0 ) * float(pricing_dict[pricing_key]['memory']) * (runTime/60.0/60.0)
        cpu_charges = ( (float(cpu)) / 1024.0 )    * float(pricing_dict[pricing_key]['cpu'])    * (runTime/60.0/60.0)

        logging.debug('In cost_of_fgtask: mem_charges=%f, cpu_charges=%f',  mem_charges, cpu_charges)
        return(mem_charges, cpu_charges)

    def _cost_of_ec2task(region, cpu, memory, ostype, instanceType, runTime, session):
        """
        https://github.com/aws-samples/amazon-ecs-chargeback/blob/master/ecs-chargeback
        Get Cost in USD to run a ECS task where launchMode==EC2.
        The AWS Pricing API returns all costs in hours. runTime is in seconds.
        """
        global pricing_dict
        global region_table

        pricing_key = '_'.join(['ec2',region, instanceType, ostype]) 
        if pricing_key not in pricing_dict:
            # Workaround for DUBLIN, Shared Tenancy and Linux
            (ec2_cpu, ec2_mem, ec2_cost) = ec2_pricing(region_table[region], instanceType, 'Shared', 'Linux', session)
            pricing_dict[pricing_key]={}
            pricing_dict[pricing_key]['cpu'] = ec2_cpu      # Number of CPUs on the EC2 instance
            pricing_dict[pricing_key]['memory'] = ec2_mem   # GiB of memory on the EC2 instance
            pricing_dict[pricing_key]['cost'] = ec2_cost    # Cost of EC2 instance (On-demand)

        # Corner case: When no CPU is assigned to a ECS Task, cpushares = 0
        # Workaround: Assume a minimum cpushare, say 128 or 256 (0.25 vcpu is the minimum on Fargate).
        if cpu == '0':
            cpu = '128'

        # Split EC2 cost bewtween memory and weights
        ec2_cpu2mem = ec2_cpu2mem_weights(pricing_dict[pricing_key]['memory'], pricing_dict[pricing_key]['cpu'])
        cpu_charges = ( (float(cpu)) / 1024.0 / pricing_dict[pricing_key]['cpu']) * ( float(pricing_dict[pricing_key]['cost']) * ec2_cpu2mem ) * (runTime/60.0/60.0)
        mem_charges = ( (float(memory)) / 1024.0 / pricing_dict[pricing_key]['memory'] ) * ( float(pricing_dict[pricing_key]['cost']) * (1.0 - ec2_cpu2mem) ) * (runTime/60.0/60.0)

        logging.debug('In cost_of_ec2task: mem_charges=%f, cpu_charges=%f',  mem_charges, cpu_charges)
        return(mem_charges, cpu_charges)

    def _get_ecs_pricing(self):
        """
        Get Fargate Pricing in the region.
        """
        svc_code = 'AmazonECS'
        response = self._prc_client.get_products(ServiceCode=svc_code, 
            Filters = [
                {'Type' :'TERM_MATCH', 'Field':'location',          'Value':region},
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
