from string import Template

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
