from typing import Any, List

from sqlalchemy import Column, MetaData, String, Table
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.base import ReadOnlyColumnCollection

from prime_vx.vcs import VCS_METADATA_KEYS


def vcsMetadataSchema(engine: Engine, tableName: str) -> None:
    metadata: MetaData = MetaData()

    table: Table = Table(
        tableName,
        metadata,
        Column("commitHash", String, primary_key=True),
        Column("treeHash", String),
        Column("parentHashes", String),
        Column("authorName", String),
        Column("authorEmail", String),
        Column("authorDate", String),
        Column("committerName", String),
        Column("committerEmail", String),
        Column("committerDate", String),
        Column("refName", String),
        Column("refNameSource", String),
        Column("gpgSignature", String),
    )

    columnData: ReadOnlyColumnCollection[str, Column[Any]] = table.columns

    # TODO: Move database table schema validation to data_model module
    if [x.key for x in columnData] == VCS_METADATA_KEYS:
        metadata.create_all(bind=engine)
    else:
        print(
            "Invalid table schema. Table schema does not align with VCS_METADATA_KEYS"
        )
        quit()
