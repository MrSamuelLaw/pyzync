class DataIntegrityError(Exception):
    """
    Error indicating that an error occured while validating an operation
    that would have corrupted data.
    """
    pass


class DataCorruptionError(Exception):
    """
    Error indicating that data corruption has occured.
    """
    pass
