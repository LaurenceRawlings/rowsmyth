from importlib.metadata import version

from ._base import TableSpecMixin, declarative_base
from ._builder import FactoryBuilder
from ._dataset import Dataset
from ._variant import variant

__version__ = version("rowsmyth")
__all__ = [
    "Dataset",
    "FactoryBuilder",
    "TableSpecMixin",
    "__version__",
    "declarative_base",
    "variant",
]
