class AWS_VM:
    def __init__(self, image_id: str, min_count: int, max_count: int,
                  instance_type: str, user_data: str, profile: dict, 
                                                    **input_kwargs):

        self.ImageId            = image_id
        self.MinCount           = min_count
        self.MaxCount           = max_count
        self.InstanceType       = instance_type
        self.UserData           = user_data
        self.IamInstanceProfile = profile
        self.input_kwargs       = input_kwargs


    def __call__(self):
        self.required_kwargs = {}
        self.required_kwargs['ImageId']            = self.ImageId           
        self.required_kwargs['MinCount']           = self.MinCount          
        self.required_kwargs['MaxCount']           = self.MaxCount          
        self.required_kwargs['InstanceType']       = self.InstanceType      
        self.required_kwargs['UserData']           = self.UserData          
        self.required_kwargs['IamInstanceProfile'] = self.IamInstanceProfile

        kwargs = {**self.required_kwargs, **self.input_kwargs}
        return kwargs


class AZURE_VM:
    def __init__(self):
        pass