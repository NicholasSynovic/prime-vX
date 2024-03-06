from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Protocol, runtime_checkable

from prime_vx.datamodels.cloc import CLOC_DF_DATAMODEL


@runtime_checkable
class CLOCTool_Protocol(Protocol):
    """
    CLOCTool_Protocol

    Top level Protocol (a class for linters to ensure that variables andmethods are defined) for count lines of code (CLOC) tool handlers to enforce common variables that are defined in all implementations
    """

    command: str
    path: Path
    toolName: str


class CLOCTool_ABC(CLOCTool_Protocol, metaclass=ABCMeta):
    """
    CLOCTool_ABC

    CLOC tool handler abstract base class (ABC) (i.e., interface) to enforce common methods are defined in all implementations. Inherits the CLOCTool_Protocol Protocol to enforce variables
    """

    @abstractmethod
    def compute(self, commitHash: str) -> CLOC_DF_DATAMODEL:
        """
        compute

        Compute the lines of code in the path. Returns a complex datatype (see return), but will default to 0 if the CLOC tool does not support the return value

        :return: A datatype containing commit hash, # of files, # of lines, # of blank lines, # of comment lines, # of code lines, dict(structured output)
        :rtype: CLOC_DF_DATAMODEL
        """
        ...
