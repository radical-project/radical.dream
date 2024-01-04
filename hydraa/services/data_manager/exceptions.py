class RcloneException(Exception):
    """
    Exception raised from rclone

    This will have the attributes:

    output - a dictionary from the call
    status - a status number
    """
    def __init__(self, output, status):
        self.output = output
        self.status = status
        message = f"Rclone command failed with status {status}: {output}"
        super().__init__(message)
