# from enum import Enum
# from functools import partial
# from datetime import date as Date
# from typing_extensions import Annotated

# from pydantic import Field, validate_call

# from pyzync.interfaces import PurePath

# @validate_call
# def _last_n_days(snapshots: list[PurePath], date: Date, n_days: Annotated[int,
#                                                                           Field(gt=0)]) -> list[bool]:
#     """Returns a bool array indicating which snapshots
#     should be deleted using the last n days algorithm
#     """
#     return [(date - ss.date).days > n_days for ss in snapshots]

# class PRUNING_ALGORITHM(Enum):
#     """A collection of named algorithms for pruning snapshots."""

#     LAST_N_DAYS = partial(_last_n_days)
