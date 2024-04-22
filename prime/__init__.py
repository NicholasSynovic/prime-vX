from collections import namedtuple
from string import Template
from typing import List

PROG: str = "PRIME vX"
TOP_LEVEL_DESCRIPTION: str = (
    "Tooling to compute process metrics of software repositories"
)
EPILOG: str = "Created by Nicholas M. Synovic"

VCS_HELP_TEMPLATE: Template = Template(
    template="Extract version control system (VCS) information from a ${vcs} software repository",
)
CLOC_HELP_TEMPLATE: Template = Template(
    template="Count lines of code of a software repository with ${tool}",
)
METRIC_HELP_TEMPLATE: Template = Template(
    template="Compute ${metric} metrics",
)
ISSUE_TRACKER_HELP_TEMPLATE: Template = Template(
    template="Get issues from ${tracker} issue tracker",
)

SUBPARSER_INFO = namedtuple(
    typename="SubparserInformation",
    field_names=["name", "description"],
)


# NOTE: EDIT THESE BELOW TO ADD NEW COMMAND LINE OPTIONS
clocSubParsers: List[SUBPARSER_INFO] = [
    SUBPARSER_INFO("cloc", "AlDanial/cloc"),
    SUBPARSER_INFO("gocloc", "hhatto/cloc"),
    SUBPARSER_INFO("sloccount", "dwheeler/cloc"),
    SUBPARSER_INFO("scc", "boyter/scc"),
]

issueTrackerSubParsers: List[SUBPARSER_INFO] = [
    SUBPARSER_INFO("gh", "GitHub"),
]

metricSubParsers: List[SUBPARSER_INFO] = [
    SUBPARSER_INFO("bf", "bus factor"),
    SUBPARSER_INFO("ic", "issue count"),
    SUBPARSER_INFO("id", "issue density"),
    SUBPARSER_INFO("is", "issue spoilage"),
    SUBPARSER_INFO("size", "project size"),
    SUBPARSER_INFO("nod", "number of developers"),
    SUBPARSER_INFO("prod", "productivity"),
]

vcsSubParsers: List[SUBPARSER_INFO] = [
    SUBPARSER_INFO("git", "git"),
    SUBPARSER_INFO("hg", "hg"),
]
