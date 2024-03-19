from argparse import ArgumentParser, HelpFormatter, Namespace, _SubParsersAction
from operator import attrgetter
from pathlib import Path
from typing import Any, List, Literal, Tuple

from prime_vx import (
    CLOC_HELP_TEMPLATE,
    EPILOG,
    METRIC_HELP_TEMPLATE,
    PROG,
    TOP_LEVEL_DESCRIPTION,
    VCS_HELP_TEMPLATE,
)
from prime_vx.cloc.main import main as clocMain
from prime_vx.exceptions import InvalidCommandLineSubprogram
from prime_vx.metrics.main import main as metricMain
from prime_vx.vcs.main import main as vcsMain


class SortingHelpFormatter(HelpFormatter):
    def add_arguments(self, actions):
        actions = sorted(actions, key=attrgetter("option_strings"))
        super(SortingHelpFormatter, self).add_arguments(actions)


class CMDLineParser:
    """
    PRIME vX specific command line parser
    """

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

        # Git VCS subparser
        self.gitSubparser: ArgumentParser = self.subparsers.add_parser(
            name="vcs-git",
            help=VCS_HELP_TEMPLATE.substitute(vcs="git"),
            prog=PROG,
            epilog=EPILOG,
            formatter_class=SortingHelpFormatter,
        )
        self._addArgs(suffix="vcs", parser=self.gitSubparser, parserName="git")

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

        # Parse args
        self.namespace: Namespace = self.parser.parse_args()

    def _addArgs(
        self,
        suffix: Literal["vcs", "cloc", "metric"],
        parser: ArgumentParser,
        parserName: str,
    ) -> None:
        helpMessage: str = ""
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
                helpMessage = (
                    f"Path to SQLite3 database generated from a {PROG} VCS tool"
                )
                destination = f"cloc.{parserName}.input"
            case "metric":
                helpMessage = (
                    f"Path to SQLite3 database generated from a {PROG} VCS tool"
                )
                destination = f"metric.{parserName}.input"
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


def main() -> None:
    parser: CMDLineParser = CMDLineParser()

    firstParameter: Tuple[str, Any] = parser.namespace._get_kwargs()[0]
    parameterData: List[str] = firstParameter[0].split(sep=".")

    match parameterData[0]:
        case "vcs":
            vcsMain(namespace=parser.namespace)
        case "cloc":
            clocMain(namespace=parser.namespace)
        case "metric":
            metricMain(namespace=parser.namespace)
        case _:
            raise InvalidCommandLineSubprogram


if __name__ == "__main__":
    main()
