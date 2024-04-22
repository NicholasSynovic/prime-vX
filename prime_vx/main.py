from argparse import ArgumentParser, HelpFormatter, Namespace, _SubParsersAction
from collections import namedtuple
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
from prime_vx.cloc import main as clocMain
from prime_vx.db.sqlite import SQLite
from prime_vx.exceptions import *
from prime_vx.exceptions import InvalidCommandLineSubprogram
from prime_vx.issue_trackers.main import main as itMain
from prime_vx.metrics.main import main as metricMain
from prime_vx.vcs.main import main as vcsMain

SUBPARSER_INFO = namedtuple(
    typename="SubparserInformation",
    field_names=["name", "description"],
)


class SortingHelpFormatter(HelpFormatter):
    def add_arguments(self, actions):
        actions = sorted(actions, key=attrgetter("option_strings"))
        super(SortingHelpFormatter, self).add_arguments(actions)


class CMDLineParser:
    def __init__(self) -> None:
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

        clocSubParsers: List[SUBPARSER_INFO] = [
            SUBPARSER_INFO("cloc", "AlDanial/cloc"),
            SUBPARSER_INFO("gocloc", "hhatto/cloc"),
            SUBPARSER_INFO("sloccount", "dwheeler/cloc"),
            SUBPARSER_INFO("scc", "boyter/scc"),
        ]

        issueTrackerSubParsers: List[SUBPARSER_INFO] = [
            SUBPARSER_INFO("gh", "GitHub"),
        ]

        metricSubParsers: List[SUBPARSER_INFO] = [
            SUBPARSER_INFO("bf", "bus factor"),
            SUBPARSER_INFO("ic", "issue count"),
            SUBPARSER_INFO("id", "issue density"),
            SUBPARSER_INFO("is", "issue spoilage"),
            SUBPARSER_INFO("size", "project size"),
            SUBPARSER_INFO("nod", "number of developers"),
            SUBPARSER_INFO("prod", "productivity"),
        ]

        vcsSubParsers: List[SUBPARSER_INFO] = [
            SUBPARSER_INFO("git", "git"),
            SUBPARSER_INFO("hg", "hg"),
        ]

        subparser: SUBPARSER_INFO
        for subparser in clocSubParsers:
            foo: ArgumentParser = self.subparsers.add_parser(
                name=f"cloc-{subparser.name}",
                help=CLOC_HELP_TEMPLATE.substitute(tool=subparser.description),
                prog=PROG,
                epilog=EPILOG,
                formatter_class=SortingHelpFormatter,
            )
            self._addArgs(
                suffix="cloc",
                parser=foo,
                parserName=subparser.name,
            )

        for subparser in issueTrackerSubParsers:
            foo: ArgumentParser = self.subparsers.add_parser(
                name=f"it-{subparser.name}",
                help=ISSUE_TRACKER_HELP_TEMPLATE.substitute(
                    tracker=subparser.description
                ),
                prog=PROG,
                epilog=EPILOG,
                formatter_class=SortingHelpFormatter,
            )
            self._addArgs(
                suffix="it",
                parser=foo,
                parserName=subparser.description,
            )

        for subparser in metricSubParsers:
            foo: ArgumentParser = self.subparsers.add_parser(
                name=f"metric-{subparser.name}",
                help=METRIC_HELP_TEMPLATE.substitute(metric=subparser.description),
                prog=PROG,
                epilog=EPILOG,
                formatter_class=SortingHelpFormatter,
            )
            self._addArgs(
                suffix="metric",
                parser=foo,
                parserName=subparser.description,
            )

        for subparser in vcsSubParsers:
            foo: ArgumentParser = self.subparsers.add_parser(
                name=f"vcs-{subparser.name}",
                help=VCS_HELP_TEMPLATE.substitute(vcs=subparser.description),
                prog=PROG,
                epilog=EPILOG,
                formatter_class=SortingHelpFormatter,
            )
            self._addArgs(
                suffix="vcs",
                parser=foo,
                parserName=subparser.description,
            )

        # Parse args
        self.namespace: Namespace = self.parser.parse_args()

    def _addArgs(
        self,
        suffix: Literal["vcs", "cloc", "metric", "it"],
        parser: ArgumentParser,
        parserName: str,
    ) -> None:
        parserName = parserName.lower().replace(" ", "_")

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

                destination = f"it.{parserName.lower()}.input"
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
