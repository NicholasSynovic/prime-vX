from argparse import ArgumentParser, HelpFormatter, Namespace, _SubParsersAction
from operator import attrgetter
from pathlib import Path
from string import Template
from typing import Any, List, Literal, Tuple

from prime_vx.vcs.main import main as vcsMain

PROG: str = "PRIME vX"
TOP_LEVEL_DESCRIPTION: str = (
    "Tooling to compute process metrics of software repositories"
)
EPILOG: str = "Created by Nicholas M. Synovic"

VCS_HELP_TEMPLATE: Template = Template(
    template="Extract version control system (VCS) information from a ${vcs} software repository",
)
CLOC_HELP_TEMPLATE: Template = Template(
    template="Compute CLOC metrics of a software repository with ${tool}",
)


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

        # Mercurial VCS subparser
        # self.mercurialSubparser: ArgumentParser = self.subparsers.add_parser(
        #     name="vcs-mercurial",
        #     help=VCS_HELP_TEMPLATE.substitute(vcs="mercurial"),
        #     prog=PROG,
        #     epilog=EPILOG,
        #     formatter_class=SortingHelpFormatter,
        # )
        # self._addArgs(
        #     suffix="vcs",
        #     parser=self.mercurialSubparser,
        #     parserName="mercurial",
        # )

        # SCC CLOC subparser
        self.sccSubparser: ArgumentParser = self.subparsers.add_parser(
            name="cloc-scc",
            help=CLOC_HELP_TEMPLATE.substitute(tool="scc"),
            prog=PROG,
            epilog=EPILOG,
            formatter_class=SortingHelpFormatter,
        )
        self._addArgs(suffix="cloc", parser=self.sccSubparser, parserName="scc")

        # SCC CLOC subparser
        # self.sccSubparser: ArgumentParser = self.subparsers.add_parser(
        #     name="cloc-cloc",
        #     help=CLOC_HELP_TEMPLATE.substitute(tool="cloc"),
        #     prog=PROG,
        #     epilog=EPILOG,
        #     formatter_class=SortingHelpFormatter,
        # )
        # self._addArgs(suffix="cloc", parser=self.sccSubparser, parserName="cloc")

        # Parse args
        self.namespace: Namespace = self.parser.parse_args()

    def _addArgs(
        self,
        suffix: Literal["vcs", "cloc"],
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
            print(parser.namespace)

        case _:
            print("Invalid command line options")
            quit(1)


if __name__ == "__main__":
    main()
