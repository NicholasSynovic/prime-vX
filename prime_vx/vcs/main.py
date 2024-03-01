from argparse import Namespace
from typing import List, Tuple


def main(namespace: Namespace) -> None:
    print(namespace)
    programInput: List[Tuple] = namespace._get_kwargs()
    pass
