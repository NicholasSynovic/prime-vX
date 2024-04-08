from argparse import ArgumentParser, HelpFormatter, Namespace, _SubParsersAction
from operator import attrgetter
from pathlib import Path
from typing import Any, List, Literal, Tuple

from pyfs import isDirectory, isFile, resolvePath

from prime_vx import (
    CLOC_HELP_TEMPLATE,
    EPILOG,
    ISSUE_TRACKER_HELP_TEMPLATE,
    METRIC_HELP_TEMPLATE,
    PROG,
    TOP_LEVEL_DESCRIPTION,
    VCS_HELP_TEMPLATE,
)
from prime_vx.cloc.main import main as clocMain
from prime_vx.db.sqlite import SQLite
from prime_vx.exceptions import *
from prime_vx.exceptions import InvalidCommandLineSubprogram
from prime_vx.issue_trackers.main import main as itMain
from prime_vx.metrics.main import main as metricMain
from prime_vx.vcs.main import main as vcsMain


class SortingHelpFormatter(HelpFormatter):
    """
    SortingHelpFormatter

    Class to order the command line arguments alphabetically
    """

    def add_arguments(self, actions):
        actions = sorted(actions, key=attrgetter("option_strings"))
        super(SortingHelpFormatter, self).add_arguments(actions)


class CMDLineParser:
    """
    PRIME vX specific command line parser
    """

    def __init__(self) -> None:
        """
        __init__

        Initialize the command line parser and handle inputs
        """
        self.parser: ArgumentParser = ArgumentParser(
            prog=PROG,
            description=TOP_LEVEL_DESCRIPTION,
            epilog=EPILOG,
            formatter_class=SortingHelpFormatter,
        )

        self.subparsers: _SubParsersAction[ArgumentParser] = self.parser.add_subparsers(
            title="Subprograms",
            description=f"{PROG} subprograms",
        )

        # SCC CLOC subparser
        self.sccSubparser: ArgumentParser = self.subparsers.add_parser(
            name="cloc-scc",
            help=CLOC_HELP_TEMPLATE.substitute(tool="scc"),
            prog=PROG,
            epilog=EPILOG,
            formatter_class=SortingHelpFormatter,
        )
        self._addArgs(
            suffix="cloc",
            parser=self.sccSubparser,
            parserName="scc",
        )

        # GitHub issue tracker subparser
        self.ghitSubparser: ArgumentParser = self.subparsers.add_parser(
            name="it-gh",
            help=ISSUE_TRACKER_HELP_TEMPLATE.substitute(tracker="GitHub"),
            prog=PROG,
            epilog=EPILOG,
            formatter_class=SortingHelpFormatter,
        )
        self._addArgs(
            suffix="it",
            parser=self.ghitSubparser,
            parserName="github",
        )

        # LOC metric subparser
        self.sccSubparser: ArgumentParser = self.subparsers.add_parser(
            name="metric-loc",
            help=METRIC_HELP_TEMPLATE.substitute(
                metric="loc, kloc, delta-loc, and delta-kloc"
            ),
            prog=PROG,
            epilog=EPILOG,
            formatter_class=SortingHelpFormatter,
        )
        self._addArgs(
            suffix="metric",
            parser=self.sccSubparser,
            parserName="loc",
        )

        # Productivity metric subparser
        self.sccSubparser: ArgumentParser = self.subparsers.add_parser(
            name="metric-prod",
            help=METRIC_HELP_TEMPLATE.substitute(metric="productivity"),
            prog=PROG,
            epilog=EPILOG,
            formatter_class=SortingHelpFormatter,
        )
        self._addArgs(
            suffix="metric",
            parser=self.sccSubparser,
            parserName="productivity",
        )

        # Git VCS subparser
        self.gitSubparser: ArgumentParser = self.subparsers.add_parser(
            name="vcs-git",
            help=VCS_HELP_TEMPLATE.substitute(vcs="git"),
            prog=PROG,
            epilog=EPILOG,
            formatter_class=SortingHelpFormatter,
        )
        self._addArgs(suffix="vcs", parser=self.gitSubparser, parserName="git")

        # Mercurial VCS subparser
        self.gitSubparser: ArgumentParser = self.subparsers.add_parser(
            name="vcs-hg",
            help=VCS_HELP_TEMPLATE.substitute(vcs="hg"),
            prog=PROG,
            epilog=EPILOG,
            formatter_class=SortingHelpFormatter,
        )
        self._addArgs(suffix="vcs", parser=self.gitSubparser, parserName="hg")

        # Parse args
        self.namespace: Namespace = self.parser.parse_args()

    def _addArgs(
        self,
        suffix: Literal["vcs", "cloc", "metric", "it"],
        parser: ArgumentParser,
        parserName: str,
    ) -> None:
        """
        _addArgs

        Functional method to handle creating command line args for different
        parsers

        :param suffix: String representing what type of parser is passed into the function
        :type suffix: Literal['vcs', 'cloc', 'metric']
        :param parser: A parser to add args
        :type parser: ArgumentParser
        :param parserName: The name of the parser to add to the help string
        :type parserName: str
        """
        helpMessage = f"Path to SQLite3 database generated from a {PROG} VCS tool"
        destination: str = ""

        match suffix:
            case "vcs":
                helpMessage = f"Path to {parserName} software repository"
                destination = f"vcs.{parserName}.input"

                parser.add_argument(
                    "-o",
                    "--output",
                    nargs=1,
                    type=Path,
                    required=True,
                    help=f"Path to output SQLite3 database",
                    dest=f"vcs.{parserName}.output",
                )
            case "cloc":
                destination = f"cloc.{parserName}.input"
            case "metric":
                destination = f"metric.{parserName}.input"
            case "it":
                parser.add_argument(
                    "-o",
                    "--owner",
                    nargs=1,
                    type=str,
                    required=True,
                    help=f"GitHub repository owner account name",
                    dest=f"it.{parserName}.owner",
                )
                parser.add_argument(
                    "-r",
                    "--repo",
                    nargs=1,
                    type=str,
                    required=True,
                    help=f"GitHub repository name",
                    dest=f"it.{parserName}.repo",
                )
                parser.add_argument(
                    "-t",
                    "--token",
                    nargs=1,
                    type=str,
                    required=True,
                    help=f"GitHub personal access token (PAT)",
                    dest=f"it.{parserName}.token",
                )

                destination = f"it.{parserName}.input"
            case _:
                pass

        parser.add_argument(
            "-i",
            "--input",
            nargs=1,
            type=Path,
            required=True,
            help=helpMessage,
            dest=destination,
        )


def getDB(namespace: Namespace, searchTerm: str = "input") -> SQLite:
    programInput: dict[str, List[Path]] = dict(namespace._get_kwargs())
    programKeys: List[str] = list(programInput.keys())

    dbKey: str = [key for key in programKeys if searchTerm in key][0]

    dbPath: Path = programInput[dbKey][0]
    resolvedDBPath: Path = resolvePath(path=dbPath)

    if isFile(path=resolvedDBPath) or not isDirectory(path=resolvedDBPath):
        pass
    else:
        raise InvalidDBPath

    db: SQLite = SQLite(path=resolvedDBPath)

    db.createTables()

    return db


def main() -> None:
    """
    main

    Main input method to the application
    """
    parser: CMDLineParser = CMDLineParser()

    try:
        firstParameter: Tuple[str, Any] = parser.namespace._get_kwargs()[0]
    except IndexError:
        print(PROG, EPILOG)
        quit(1)

    parameterData: List[str] = firstParameter[0].split(sep=".")

    db: SQLite
    match parameterData[0]:
        case "vcs":
            db = getDB(
                namespace=parser.namespace,
                searchTerm="output",
            )

            vcsMain(
                namespace=parser.namespace,
                db=db,
            )
        case "cloc":
            db = getDB(namespace=parser.namespace)

            clocMain(
                namespace=parser.namespace,
                db=db,
            )
        case "metric":
            db = getDB(namespace=parser.namespace)

            metricMain(
                namespace=parser.namespace,
                db=db,
            )
        case "it":
            db = getDB(namespace=parser.namespace)

            itMain(
                namespace=parser.namespace,
                db=db,
            )
        case _:
            raise InvalidCommandLineSubprogram


if __name__ == "__main__":
    main()
