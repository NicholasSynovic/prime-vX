from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import List, Protocol

from pandas import DataFrame

from prime_vx.datamodels.vcs import VCS_DF_DATAMODEL


class VCSHandler_Protocol(Protocol):
    """
    VCSHandler_Protocol

    Top level Protocol (a class for linters to ensure that variables andmethods are defined) for version control system (VCS) handlers to enforce common variables are defined in all implementations
    """

    path: Path


class VCSHandler_ABC(VCSHandler_Protocol, metaclass=ABCMeta):
    """
    VCSHandler_ABC

    VCS handler abstract base class (ABC) (i.e., interface) to enforce common methods are defined in all implementations. Inherits the VCSHandler_Protocol Protocol to enforce variables
    """

    @abstractmethod
    def checkoutCommit(self, commitHash: str) -> bool:
        """
        checkoutCommit

        Checkout specific commit hash within the repository

        :param commitHash: Commit hash to checkout
        :type commitHash: str
        :return: True if the commit checkout was successful; else, False
        :rtype: bool
        """
        ...

    @abstractmethod
    def getCommitHashes(self) -> List[str]:
        """
        getCommitHashes

        Get the list of hashes across all branches within the repository

        :return: A list of commit hashes
        :rtype: List[str]
        """
        ...

    @abstractmethod
    def getCommitMetadata(self, commitHash: str) -> VCS_DF_DATAMODEL:
        """
        getCommitMetadata

        Get the relevant metadata of a commit as defined in prime_vx.vcs.VCS_METADATA_KEY_LIST

        :param commitHash: A commit hash from the repository
        :type commitHash: str
        :return: The relevant metadata as defined by prime_vx.vcs.VCS_METADATA_KEY_LIST
        :rtype: DataFrame
        """
        ...

    @abstractmethod
    def isRepository(self) -> bool:
        """
        isRepository

        Determine if the current path is a valid repository to analyze

        :return: True if the path is a repository; else, False
        :rtype: bool
        """
        ...
