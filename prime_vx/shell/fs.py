from os.path import abspath, expanduser, isdir, isfile
from pathlib import Path


def resolvePath(path: Path) -> Path:
    """
    resolvePath

    Convert path from a relative path of the Python program execution to an absolute path

    :param path: Path to resolve to an absolute path
    :type path: Path
    :return: An absolute path
    :rtype: Path
    """
    return Path(abspath(path=expanduser(path=path)))


def isFile(path: Path) -> bool:
    """
    isFile

    An extension of the os.path.isfile() function that uses the absolute value of a path rather than the relative path

    :param path: Path to check if it is a file
    :type path: Path
    :return: True if the absolute path is a file; else return False
    :rtype: bool
    """
    return isfile(path=resolvePath(path=path))


def isDirectory(path: Path) -> bool:
    """
    isDirectory

    An extension of the os.path.isdir() function that uses the absolute value of a path rather than the relative path

    :param path: Path to check if it is a directory
    :type path: Path
    :return: True if the absolute path is a directory; else return False
    :rtype: bool
    """
    return isdir(s=resolvePath(path=path))
