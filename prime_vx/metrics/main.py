from argparse import Namespace
from pathlib import Path
from typing import List

from pandas import DataFrame

from prime_vx.datamodels.cloc import CLOC_DF_DATAMODEL
from prime_vx.db import CLOC_DB_TABLE_NAME
from prime_vx.db.sqlite import Generic_DB
from prime_vx.exceptions import InvalidDBPath, InvalidMetricSubprogram
from prime_vx.metrics.loc.main import main as locMain
from prime_vx.shell.fs import isFile, resolvePath

def main(namespace: Namespace) -> None:
    # TODO: Add docstring

    programInput: dict[str, List[Path]] = dict(namespace._get_kwargs())
    programKeys: List[str] = list(programInput.keys())

    inputKey: str = [key for key in programKeys if "input" in key][0]
    inputKeySplit: List[str] = inputKey.split(sep=".")

    dbPath: Path = programInput[inputKey][0]
    resolvedDBPath: Path = resolvePath(path=dbPath)

    if isFile(path=resolvedDBPath):
        pass
    else:
        raise InvalidDBPath

    db: Generic_DB = Generic_DB(path=resolvedDBPath)
    clocDF: DataFrame = db.read(
        tdf=CLOC_DF_DATAMODEL,
        tableName=CLOC_DB_TABLE_NAME,
    )

    metricName: str = inputKeySplit[1]

    match metricName:
        case "loc":
            df: DataFrame = locMain(df=clocDF)
            db.write(df=df, tableName="loc", includeIndex=True)
        case _:
            raise InvalidMetricSubprogram
