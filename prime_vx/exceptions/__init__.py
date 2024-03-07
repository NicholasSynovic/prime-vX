class InvalidCommandLineSubprogram(Exception):
    def __init__(self) -> None:
        self.message = "Invalid command line subprogram arguement"
        super().__init__(self.message)


class InvalidVersionControl(Exception):
    def __init__(self) -> None:
        self.message = "Invalid version control"
        super().__init__(self.message)
