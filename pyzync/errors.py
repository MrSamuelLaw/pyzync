class DataIntegrityError(Exception):
    """
    Exception raised when a requested operation would violate data integrity.
    """


class DataCorruptionError(Exception):
    """
    Exception raised when the requested operation could not be completed due to corrupted or missing data.
    """
