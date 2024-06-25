import re
from datetime import datetime
from json import dumps
from re import Match
from string import Template
from time import sleep, time
from typing import List

from numpy import datetime64
from pandas import DataFrame, Timestamp
from progress.bar import Bar
from requests import Response, get
from requests.structures import CaseInsensitiveDict

from prime.datamodels.issue_tracker import IT_DF_DATAMODEL
from prime.issue_trackers import RESPONSE_HEADERS_HANDLER
from prime.issue_trackers._classes._issueTrackerHandler import ITHandler_ABC


class GitHubHandler(ITHandler_ABC):
    def __init__(self, owner: str, repo: str, token: str) -> None:
        self.name: str = "github"
        self.token: str = token

        foo: str = f"https://api.github.com/repos/{owner}/{repo}/issues?state=all&direction=asc&per_page=100"
        self.endpoint: Template = Template(template=foo + "&page=${page}")

    def parseResponseHeader(self, headers: CaseInsensitiveDict) -> None:
        link: str = headers["link"]
        splitLink: List[str] = link.split(sep=",")

        lastPageLink: str = ""
        if len(splitLink) == 2:
            lastPageLink = splitLink[1]
        else:
            lastPageLink = splitLink[2]

        lastPageMatch: Match[str] | None = re.search(r"[?&]page=(\d+)", lastPageLink)
        lastPage: int | None = int(lastPageMatch.group(1)) if lastPageMatch else None

        RESPONSE_HEADERS_HANDLER["lastPage"] = lastPage
        RESPONSE_HEADERS_HANDLER["tokenLimit"] = int(headers["x-ratelimit-limit"])
        RESPONSE_HEADERS_HANDLER["tokenRemaining"] = int(
            headers["x-ratelimit-remaining"]
        )
        RESPONSE_HEADERS_HANDLER["tokenReset"] = int(headers["x-ratelimit-reset"]) + 10

    def getResponses(self) -> List[Response]:
        data: List[Response] = []

        headers: dict[str, str] = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "prime-vX",
            "Authorization": f"Bearer {self.token}",
        }

        with Bar("Getting issues...", max=1) as bar:

            def _get(page: int) -> bool:
                resp: Response = get(
                    url=self.endpoint.substitute(page=page),
                    headers=headers,
                    timeout=60,
                )
                data.append(resp)

                if resp.status_code != 200:
                    bar.next()
                    return False
                else:
                    self.parseResponseHeader(headers=resp.headers)
                    return True

            if _get(page=1) == False:
                return data

            bar.max = RESPONSE_HEADERS_HANDLER["lastPage"]
            bar.update()
            bar.next()

            stableLastPage: int = RESPONSE_HEADERS_HANDLER["lastPage"]

            page: int
            for page in range(2, stableLastPage + 1):
                if RESPONSE_HEADERS_HANDLER["tokenRemaining"] > 0:
                    pass
                else:
                    currentTime: float = time()
                    diffTime: float = (
                        RESPONSE_HEADERS_HANDLER["tokenReset"] - currentTime
                    )
                    sleepUntil: datetime = datetime.fromtimestamp(
                        RESPONSE_HEADERS_HANDLER["tokenReset"]
                    )
                    message: str = f"Sleeping until {sleepUntil}..."
                    bar.message = message
                    bar.update()
                    sleep(diffTime)
                    bar.message = "Getting issues..."
                    bar.update()

                if _get(page=page) == False:
                    break
                else:
                    bar.next()

        return data

    def extractIssues(self, resps: List[Response]) -> DataFrame:
        data: dict[str, List[str | int | float]] = {
            "id": [],
            "node_id": [],
            "number": [],
            "state": [],
            "date_opened": [],
            "date_closed": [],
            "issue_tracker": [],
            "url": [],
            "json": [],
        }

        with Bar("Extracting issues from HTTP responses...", max=len(resps)) as bar:
            resp: Response
            for resp in resps:
                json: List[dict] = resp.json()

                datum: dict
                for datum in json:
                    createdAt: datetime64 = Timestamp(
                        ts_input=datum["created_at"]
                    ).to_datetime64()

                    closedAt: datetime64
                    try:
                        closedAt = Timestamp(
                            ts_input=datum["closed_at"]
                        ).to_datetime64()
                    except TypeError:
                        closedAt = Timestamp().to_datetime64()

                    data["id"].append(datum["id"])
                    data["node_id"].append(datum["node_id"])
                    data["number"].append(datum["number"])
                    data["state"].append(datum["state"])
                    data["date_opened"].append(createdAt)
                    data["date_closed"].append(closedAt)
                    data["issue_tracker"].append(self.name)
                    data["url"].append(resp.url)
                    data["json"].append(dumps(obj=datum))

                bar.next()

        return IT_DF_DATAMODEL(df=DataFrame(data=data)).df
