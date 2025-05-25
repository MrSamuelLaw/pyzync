class DataIntegrityError(Exception):
    """Used to indicate that a requested operation would violate date integrity."""


class DataCorruptionError(Exception):
    """Used to indicate that the requested operation could not be completed due to corrupted/missing data."""
