from pathlib import Path

import pandas
from pandas import DataFrame
from pyfs import isDirectory, isFile, resolvePath
from sqlalchemy import (
    Column,
    DateTime,
    Engine,
    Float,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    create_engine,
    event,
)
from sqlalchemy.exc import IntegrityError
from typedframe import TypedDataFrame

from prime_vx.db import *
from prime_vx.db._classes._dbHandler import SQLiteHandler_ABC
from prime_vx.exceptions import InvalidDBPath


class SQLite(SQLiteHandler_ABC):
    def __init__(self, path: Path) -> None:
        resolvedPath: Path = resolvePath(path=path)

        if isDirectory(path=resolvedPath):
            raise InvalidDBPath

        self.path = resolvedPath

        if isFile(path=self.path):
            self.exists = True
        else:
            self.exists = False

        self.engine = create_engine(url=f"sqlite:///{self.path}")

        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    def createTables(self) -> None:
        metadata: MetaData = MetaData()

        vcsTable: Table = Table(
            VCS_DB_TABLE_NAME,
            metadata,
            Column("commit_hash", String),
            Column("tree_hash", String),
            Column("parent_hash", String),
            Column("author_name", String),
            Column("author_email", String),
            Column("author_date", DateTime),
            Column("committer_name", String),
            Column("committer_email", String),
            Column("committer_date", DateTime),
            Column("ref_name", String),
            Column("ref_name_source", String),
            Column("gpg_signature", String),
            Column("vcs", String),
            Column("path", String),
            PrimaryKeyConstraint("commit_hash"),
        )

        clocTable: Table = Table(
            CLOC_DB_TABLE_NAME,
            metadata,
            Column("index", Integer),
            Column("commit_hash", String),
            Column("file_count", Integer),
            Column("line_count", Integer),
            Column("blank_line_count", Integer),
            Column("comment_line_count", Integer),
            Column("code_line_count", Integer),
            Column("tool", String),
            Column("json", String),
            PrimaryKeyConstraint("index"),
            ForeignKeyConstraint(
                columns=["commit_hash"],
                refcolumns=[f"{VCS_DB_TABLE_NAME}.commit_hash"],
            ),
        )

        locTable: Table = Table(
            LOC_DB_TABLE_NAME,
            metadata,
            Column("index", Integer),
            Column("commit_hash", String),
            Column("loc", Integer),
            Column("kloc", Float),
            Column("delta_loc", Integer),
            Column("delta_kloc", Float),
            PrimaryKeyConstraint("index"),
            ForeignKeyConstraint(
                columns=["commit_hash"],
                refcolumns=[f"{VCS_DB_TABLE_NAME}.commit_hash"],
            ),
        )

        issueTrackerTable: Table = Table(
            ISSUE_TRACKER_DB_TABLE_NAME,
            metadata,
            Column("id", Integer),
            Column("node_id", String),
            Column("number", Integer),
            Column("state", String),
            Column("date_opened", DateTime),
            Column("date_closed", DateTime),
            Column("url", String),
            Column("json", String),
            PrimaryKeyConstraint("id"),
        )

        dailyProductivityTable: Table = Table(
            DAILY_PRODUCTIVITY_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("effort_loc", Integer),
            Column("effort_kloc", Float),
            Column("productivity_loc", Float),
            Column("productivity_kloc", Float),
            PrimaryKeyConstraint("bucket"),
        )

        weeklyProductivityTable: Table = Table(
            WEEKLY_PRODUCTIVITY_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("effort_loc", Integer),
            Column("effort_kloc", Float),
            Column("productivity_loc", Float),
            Column("productivity_kloc", Float),
            PrimaryKeyConstraint("bucket"),
        )

        twoWeekProductivityTable: Table = Table(
            TWO_WEEK_PRODUCTIVITY_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("effort_loc", Integer),
            Column("effort_kloc", Float),
            Column("productivity_loc", Float),
            Column("productivity_kloc", Float),
            PrimaryKeyConstraint("bucket"),
        )

        monthlyProductivityTable: Table = Table(
            MONTHLY_PRODUCTIVITY_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("effort_loc", Integer),
            Column("effort_kloc", Float),
            Column("productivity_loc", Float),
            Column("productivity_kloc", Float),
            PrimaryKeyConstraint("bucket"),
        )

        twoMonthProductivityTable: Table = Table(
            TWO_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("effort_loc", Integer),
            Column("effort_kloc", Float),
            Column("productivity_loc", Float),
            Column("productivity_kloc", Float),
            PrimaryKeyConstraint("bucket"),
        )

        threeMonthProductivityTable: Table = Table(
            THREE_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("effort_loc", Integer),
            Column("effort_kloc", Float),
            Column("productivity_loc", Float),
            Column("productivity_kloc", Float),
            PrimaryKeyConstraint("bucket"),
        )

        sixMonthProductivityTable: Table = Table(
            SIX_MONTH_PRODUCTIVITY_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("effort_loc", Integer),
            Column("effort_kloc", Float),
            Column("productivity_loc", Float),
            Column("productivity_kloc", Float),
            PrimaryKeyConstraint("bucket"),
        )

        annualMonthProductivityTable: Table = Table(
            ANNUAL_PRODUCTIVITY_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("effort_loc", Integer),
            Column("effort_kloc", Float),
            Column("productivity_loc", Float),
            Column("productivity_kloc", Float),
            PrimaryKeyConstraint("bucket"),
        )

        commitHashToProductivityBucketMap: Table = Table(
            COMMIT_HASH_TO_PRODUCTIVITY_BUCKET_MAP_TABLE_NAME,
            metadata,
            Column("index", Integer),
            Column("commit_hash", String),
            Column(DAILY_PRODUCTIVITY_DB_TABLE_NAME, Integer),
            Column(WEEKLY_PRODUCTIVITY_DB_TABLE_NAME, Integer),
            Column(TWO_WEEK_PRODUCTIVITY_DB_TABLE_NAME, Integer),
            Column(MONTHLY_PRODUCTIVITY_DB_TABLE_NAME, Integer),
            Column(TWO_MONTH_PRODUCTIVITY_DB_TABLE_NAME, Integer),
            Column(THREE_MONTH_PRODUCTIVITY_DB_TABLE_NAME, Integer),
            Column(SIX_MONTH_PRODUCTIVITY_DB_TABLE_NAME, Integer),
            Column(ANNUAL_PRODUCTIVITY_DB_TABLE_NAME, Integer),
            PrimaryKeyConstraint("index"),
            ForeignKeyConstraint(
                columns=["commit_hash"],
                refcolumns=[f"{VCS_DB_TABLE_NAME}.commit_hash"],
            ),
            ForeignKeyConstraint(
                columns=[DAILY_PRODUCTIVITY_DB_TABLE_NAME],
                refcolumns=[f"{DAILY_PRODUCTIVITY_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[WEEKLY_PRODUCTIVITY_DB_TABLE_NAME],
                refcolumns=[f"{WEEKLY_PRODUCTIVITY_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[TWO_WEEK_PRODUCTIVITY_DB_TABLE_NAME],
                refcolumns=[f"{TWO_WEEK_PRODUCTIVITY_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[MONTHLY_PRODUCTIVITY_DB_TABLE_NAME],
                refcolumns=[f"{MONTHLY_PRODUCTIVITY_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[TWO_MONTH_PRODUCTIVITY_DB_TABLE_NAME],
                refcolumns=[f"{TWO_MONTH_PRODUCTIVITY_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[THREE_MONTH_PRODUCTIVITY_DB_TABLE_NAME],
                refcolumns=[
                    f"{THREE_MONTH_PRODUCTIVITY_DB_TABLE_NAME}.bucket",
                ],
            ),
            ForeignKeyConstraint(
                columns=[SIX_MONTH_PRODUCTIVITY_DB_TABLE_NAME],
                refcolumns=[f"{SIX_MONTH_PRODUCTIVITY_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[ANNUAL_PRODUCTIVITY_DB_TABLE_NAME],
                refcolumns=[f"{ANNUAL_PRODUCTIVITY_DB_TABLE_NAME}.bucket"],
            ),
        )

        dailyDeveloperCountTable: Table = Table(
            DAILY_DEVELOPER_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("developer_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        weeklyDeveloperCountTable: Table = Table(
            WEEKLY_DEVELOPER_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("developer_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        twoWeekDeveloperCountTable: Table = Table(
            TWO_WEEK_DEVELOPER_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("developer_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        monthlyDeveloperCountTable: Table = Table(
            MONTHLY_DEVELOPER_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("developer_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        twoMonthDeveloperCountTable: Table = Table(
            TWO_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("developer_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        threeMonthDeveloperCountTable: Table = Table(
            THREE_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("developer_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        sixMonthDeveloperCountTable: Table = Table(
            SIX_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("developer_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        annualMonthDeveloperCountTable: Table = Table(
            ANNUAL_DEVELOPER_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("developer_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        commitHashToDeveloperCountBucketMap: Table = Table(
            COMMIT_HASH_TO_DEVELOPER_COUNT_BUCKET_MAP_TABLE_NAME,
            metadata,
            Column("index", Integer),
            Column("commit_hash", String),
            Column(DAILY_DEVELOPER_COUNT_DB_TABLE_NAME, Integer),
            Column(WEEKLY_DEVELOPER_COUNT_DB_TABLE_NAME, Integer),
            Column(TWO_WEEK_DEVELOPER_COUNT_DB_TABLE_NAME, Integer),
            Column(MONTHLY_DEVELOPER_COUNT_DB_TABLE_NAME, Integer),
            Column(TWO_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME, Integer),
            Column(THREE_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME, Integer),
            Column(SIX_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME, Integer),
            Column(ANNUAL_DEVELOPER_COUNT_DB_TABLE_NAME, Integer),
            PrimaryKeyConstraint("index"),
            ForeignKeyConstraint(
                columns=["commit_hash"],
                refcolumns=[f"{VCS_DB_TABLE_NAME}.commit_hash"],
            ),
            ForeignKeyConstraint(
                columns=[DAILY_DEVELOPER_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{DAILY_DEVELOPER_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[WEEKLY_DEVELOPER_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{WEEKLY_DEVELOPER_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[TWO_WEEK_DEVELOPER_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{TWO_WEEK_DEVELOPER_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[MONTHLY_DEVELOPER_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{MONTHLY_DEVELOPER_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[TWO_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{TWO_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[THREE_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{THREE_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[SIX_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{SIX_MONTH_DEVELOPER_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[ANNUAL_DEVELOPER_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{ANNUAL_DEVELOPER_COUNT_DB_TABLE_NAME}.bucket"],
            ),
        )

        dailyIssueCountTable: Table = Table(
            DAILY_ISSUE_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        weeklyIssueCountTable: Table = Table(
            WEEKLY_ISSUE_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        twoWeekIssueCountTable: Table = Table(
            TWO_WEEK_ISSUE_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        monthlyIssueCountTable: Table = Table(
            MONTHLY_ISSUE_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        twoMonthIssueCountTable: Table = Table(
            TWO_MONTH_ISSUE_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        threeMonthIssueCountTable: Table = Table(
            THREE_MONTH_ISSUE_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        sixMonthIssueCountTable: Table = Table(
            SIX_MONTH_ISSUE_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        annualMonthIssueCountTable: Table = Table(
            ANNUAL_ISSUE_COUNT_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_count", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        commitHashToIssueCountBucketMap: Table = Table(
            ISSUE_ID_TO_ISSUE_COUNT_BUCKET_MAP_TABLE_NAME,
            metadata,
            Column("index", Integer),
            Column("issue_id", String),
            Column(DAILY_ISSUE_COUNT_DB_TABLE_NAME, Integer),
            Column(WEEKLY_ISSUE_COUNT_DB_TABLE_NAME, Integer),
            Column(TWO_WEEK_ISSUE_COUNT_DB_TABLE_NAME, Integer),
            Column(MONTHLY_ISSUE_COUNT_DB_TABLE_NAME, Integer),
            Column(TWO_MONTH_ISSUE_COUNT_DB_TABLE_NAME, Integer),
            Column(THREE_MONTH_ISSUE_COUNT_DB_TABLE_NAME, Integer),
            Column(SIX_MONTH_ISSUE_COUNT_DB_TABLE_NAME, Integer),
            Column(ANNUAL_ISSUE_COUNT_DB_TABLE_NAME, Integer),
            PrimaryKeyConstraint("index"),
            ForeignKeyConstraint(
                columns=["issue_id"],
                refcolumns=[f"{ISSUE_TRACKER_DB_TABLE_NAME}.id"],
            ),
            ForeignKeyConstraint(
                columns=[DAILY_ISSUE_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{DAILY_ISSUE_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[WEEKLY_ISSUE_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{WEEKLY_ISSUE_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[TWO_WEEK_ISSUE_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{TWO_WEEK_ISSUE_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[MONTHLY_ISSUE_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{MONTHLY_ISSUE_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[TWO_MONTH_ISSUE_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{TWO_MONTH_ISSUE_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[THREE_MONTH_ISSUE_COUNT_DB_TABLE_NAME],
                refcolumns=[
                    f"{THREE_MONTH_ISSUE_COUNT_DB_TABLE_NAME}.bucket",
                ],
            ),
            ForeignKeyConstraint(
                columns=[SIX_MONTH_ISSUE_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{SIX_MONTH_ISSUE_COUNT_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[ANNUAL_ISSUE_COUNT_DB_TABLE_NAME],
                refcolumns=[f"{ANNUAL_ISSUE_COUNT_DB_TABLE_NAME}.bucket"],
            ),
        )

        dailyIssueSpoilageTable: Table = Table(
            DAILY_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("spoiled_issues", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        weeklyIssueSpoilageTable: Table = Table(
            WEEKLY_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("spoiled_issues", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        twoWeekIssueSpoilageTable: Table = Table(
            TWO_WEEK_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("spoiled_issues", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        monthlyIssueSpoilageTable: Table = Table(
            MONTHLY_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("spoiled_issues", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        twoMonthIssueSpoilageTable: Table = Table(
            TWO_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("spoiled_issues", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        threeMonthIssueSpoilageTable: Table = Table(
            THREE_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("spoiled_issues", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        sixMonthIssueSpoilageTable: Table = Table(
            SIX_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("spoiled_issues", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        annualMonthIssueSpoilageTable: Table = Table(
            ANNUAL_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("spoiled_issues", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        dailyIssueDensityTable: Table = Table(
            DAILY_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_density", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        weeklyIssueDensityTable: Table = Table(
            WEEKLY_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_density", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        twoWeekIssueDensityTable: Table = Table(
            TWO_WEEK_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_density", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        monthlyIssueDensityTable: Table = Table(
            MONTHLY_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_density", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        twoMonthIssueDensityTable: Table = Table(
            TWO_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_density", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        threeMonthIssueDensityTable: Table = Table(
            THREE_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_density", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        sixMonthIssueDensityTable: Table = Table(
            SIX_MONTH_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_density", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        annualMonthIssueDensityTable: Table = Table(
            ANNUAL_ISSUE_SPOILAGE_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("issue_density", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        dailyBusFactorTable: Table = Table(
            DAILY_BUS_FACTOR_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("bus_factor", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        weeklyBusFactorTable: Table = Table(
            WEEKLY_BUS_FACTOR_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("bus_factor", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        twoWeekBusFactorTable: Table = Table(
            TWO_WEEK_BUS_FACTOR_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("bus_factor", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        monthlyBusFactorTable: Table = Table(
            MONTHLY_BUS_FACTOR_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("bus_factor", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        twoMonthBusFactorTable: Table = Table(
            TWO_MONTH_BUS_FACTOR_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("bus_factor", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        threeMonthBusFactorTable: Table = Table(
            THREE_MONTH_BUS_FACTOR_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("bus_factor", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        sixMonthBusFactorTable: Table = Table(
            SIX_MONTH_BUS_FACTOR_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("bus_factor", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        annualMonthBusFactorTable: Table = Table(
            ANNUAL_BUS_FACTOR_DB_TABLE_NAME,
            metadata,
            Column("bucket", Integer),
            Column("bucket_start", DateTime),
            Column("bucket_end", DateTime),
            Column("bus_factor", Integer),
            PrimaryKeyConstraint("bucket"),
        )

        commitHashToBusFactorBucketMap: Table = Table(
            COMMIT_HASH_TO_BUS_FACTOR_BUCKET_MAP_TABLE_NAME,
            metadata,
            Column("index", Integer),
            Column("commit_hash", String),
            Column(DAILY_BUS_FACTOR_DB_TABLE_NAME, Integer),
            Column(WEEKLY_BUS_FACTOR_DB_TABLE_NAME, Integer),
            Column(TWO_WEEK_BUS_FACTOR_DB_TABLE_NAME, Integer),
            Column(MONTHLY_BUS_FACTOR_DB_TABLE_NAME, Integer),
            Column(TWO_MONTH_BUS_FACTOR_DB_TABLE_NAME, Integer),
            Column(THREE_MONTH_BUS_FACTOR_DB_TABLE_NAME, Integer),
            Column(SIX_MONTH_BUS_FACTOR_DB_TABLE_NAME, Integer),
            Column(ANNUAL_BUS_FACTOR_DB_TABLE_NAME, Integer),
            PrimaryKeyConstraint("index"),
            ForeignKeyConstraint(
                columns=["commit_hash"],
                refcolumns=[f"{VCS_DB_TABLE_NAME}.commit_hash"],
            ),
            ForeignKeyConstraint(
                columns=[DAILY_BUS_FACTOR_DB_TABLE_NAME],
                refcolumns=[f"{DAILY_BUS_FACTOR_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[WEEKLY_BUS_FACTOR_DB_TABLE_NAME],
                refcolumns=[f"{WEEKLY_BUS_FACTOR_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[TWO_WEEK_BUS_FACTOR_DB_TABLE_NAME],
                refcolumns=[f"{TWO_WEEK_BUS_FACTOR_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[MONTHLY_BUS_FACTOR_DB_TABLE_NAME],
                refcolumns=[f"{MONTHLY_BUS_FACTOR_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[TWO_MONTH_BUS_FACTOR_DB_TABLE_NAME],
                refcolumns=[f"{TWO_MONTH_BUS_FACTOR_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[THREE_MONTH_BUS_FACTOR_DB_TABLE_NAME],
                refcolumns=[f"{THREE_MONTH_BUS_FACTOR_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[SIX_MONTH_BUS_FACTOR_DB_TABLE_NAME],
                refcolumns=[f"{SIX_MONTH_BUS_FACTOR_DB_TABLE_NAME}.bucket"],
            ),
            ForeignKeyConstraint(
                columns=[ANNUAL_BUS_FACTOR_DB_TABLE_NAME],
                refcolumns=[f"{ANNUAL_BUS_FACTOR_DB_TABLE_NAME}.bucket"],
            ),
        )

        metadata.create_all(bind=self.engine, checkfirst=True)

    def write(self, df: DataFrame, tableName: str, includeIndex: bool = False) -> None:
        try:
            df.to_sql(
                name=tableName,
                con=self.engine,
                index=includeIndex,
                index_label="index",
                if_exists="append",
            )
        except IntegrityError:
            pass

    def read(self, tdf: type[TypedDataFrame], tableName: str) -> DataFrame:
        df: DataFrame = pandas.read_sql_table(
            table_name=tableName,
            con=self.engine,
        )

        return tdf(df=df).df
