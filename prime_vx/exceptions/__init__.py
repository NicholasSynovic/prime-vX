class InvalidCommandLineSubprogram(Exception):
    def __init__(self) -> None:
        self.message = "Invalid command line subprogram arguement"
        super().__init__(self.message)
