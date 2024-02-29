from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path
from string import Template

PROG: str = "PRIME vX"
TOP_LEVEL_DESCRIPTION: str = (
    "Tooling to compute process metrics of software repositories"
)
EPILOG: str = "Created by Nicholas M. Synovic"


class CMDLineParser:
    """
    PRIME vX specific command line parser
    """

    def __init__(self) -> None:
        self.parser: ArgumentParser = ArgumentParser(
            prog=PROG,
            description=TOP_LEVEL_DESCRIPTION,
            epilog=EPILOG,
        )

        self.vcsSubparsers: _SubParsersAction[
            ArgumentParser
        ] = self.parser.add_subparsers(
            title="VCS",
            description="Subcommands to extract data from different version control systems",
        )

        # Config subparsers via functions
        self._configVCSParser()

        # Parse args
        self.namespace: Namespace = self.parser.parse_args()

    def _configVCSParser(self) -> None:
        def _addArgs(parser: ArgumentParser, vcsName: str) -> None:
            parser.add_argument(
                "-i",
                "--input",
                nargs=1,
                type=Path,
                required=True,
                help=f"Path to {vcsName} repository",
                dest=f"vcs.{vcsName}.input",
            )

            parser.add_argument(
                "-o",
                "--output",
                nargs=1,
                type=Path,
                required=True,
                help=f"Path to output SQLite3 database",
                dest=f"vcs.{vcsName}.output",
            )

        descriptionTemplate: Template = Template(
            template="Tools to extract data from ${vcs} repositories"
        )

        gitParser: ArgumentParser = self.vcsSubparsers.add_parser(
            name="git",
            description=descriptionTemplate.substitute(vcs="git"),
            epilog=EPILOG,
        )

        mercurialParser: ArgumentParser = self.vcsSubparsers.add_parser(
            name="mercurial",
            description=descriptionTemplate.substitute(vcs="mercurial"),
            epilog=EPILOG,
        )

        _addArgs(parser=gitParser, vcsName="git")
        _addArgs(parser=mercurialParser, vcsName="mercurial")


def main() -> None:
    parser: CMDLineParser = CMDLineParser()
    print(parser.namespace)


if __name__ == "__main__":
    main()
