from subprocess import PIPE, CompletedProcess, run


def runProgram(cmd: str) -> CompletedProcess[bytes]:
    """
    runProgram

    Execute shell command or program in the current process of the Python script. Python program execution will continue after the shell command has completed or exited

    :param cmd: Shell command or program to execute
    :type cmd: str
    :return: A CompletedProcess object that allows for analysis of stderr and stdout
    :rtype: CompletedProcess[bytes]
    """
    execution: CompletedProcess[bytes] = run(
        args=cmd.split(sep=" "),
        stdout=PIPE,
        stderr=PIPE,
    )
    return execution
