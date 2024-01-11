class RcloneException(Exception):
    """
    Exception raised from rclone

    This will have the attributes:

    output - a dictionary from the call
    status - a status number
    """
    def __init__(self, reason, output):
        self.reason = reason
        self.output = output

        message = f"rclone command failed: {reason}: {output}"
        super().__init__(message)
