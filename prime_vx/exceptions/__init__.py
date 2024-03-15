from prime_vx.cloc import CLOC_KEY_LIST
from prime_vx.vcs import VCS_METADATA_KEY_LIST


class InvalidCommandLineSubprogram(Exception):
    def __init__(self) -> None:
        self.message = "Invalid command line subprogram arguement"
        super().__init__(self.message)


class InvalidMetricSubprogram(Exception):
    def __init__(self) -> None:
        self.message = "Invalid metric subprogram"
        super().__init__(self.message)


class InvalidVersionControl(Exception):
    def __init__(self) -> None:
        self.message = "Invalid version control system"
        super().__init__(self.message)


class InvalidVCSTableSchema(Exception):
    def __init__(self) -> None:
        self.message = f"Invalid table schema. Schema does not match the following columns: {VCS_METADATA_KEY_LIST}"
        super().__init__(self.message)


class InvalidCLOCTableSchema(Exception):
    def __init__(self) -> None:
        self.message = f"Invalid table schema. Schema does not match the following columns: {CLOC_KEY_LIST}"
        super().__init__(self.message)


class InvalidDBPath(Exception):
    def __init__(self) -> None:
        self.message = (
            f"Provided path points to directory. Please point to database file"
        )
        super().__init__(self.message)


class InvalidDirectoryPath(Exception):
    def __init__(self) -> None:
        self.message = f"Provided path does not point to a valid directory. Please point to directory"
        super().__init__(self.message)


class InvalidCLOCTool(Exception):
    def __init__(self) -> None:
        self.message = "Invalid CLOC-like metric computation tool"
        super().__init__(self.message)


class VCSDBError_MultipleVCSCaptured(Exception):
    def __init__(self) -> None:
        self.message = "ERROR: Multiple VCSs captured in VCS database"
        super().__init__(self.message)


class VCSDBError_MultiplePathCaptured(Exception):
    def __init__(self) -> None:
        self.message = "ERROR: Multiple project paths captured in VCS database"
        super().__init__(self.message)
