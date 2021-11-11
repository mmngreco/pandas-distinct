from ._version import get_versions
import pandas as pd
from .core import distinct

__version__ = get_versions()['version']
del get_versions

pd.distinct = distinct
