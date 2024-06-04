import math
from pathlib import Path
from typing import Path

import matplotlib.pyplot as plt
import seaborn
from matplotlib.axes import Axes
from matplotlib.text import Text
from matplotlib.ticker import MultipleLocator
from pandas import DataFrame


def bar(
    projectName: str,
    table: str,
    df: DataFrame,
    figPath: Path,
    y: str,
) -> None:
    title: str = " ".join([word.capitalize() for word in table.split("_")])
    spacing: int = math.floor(math.log(df["bucket"].max(), 10))

    seaborn.set_theme()

    graph: Axes = seaborn.barplot(x="bucket", y=y, data=df)
    graph.set_xlabel("Bucket")
    graph.set_ylabel(" ".join([word.capitalize() for word in y.split("_")]))

    plt.title(f" {title} of {projectName}")

    idx: int
    label: Text
    for idx, label in list(enumerate(graph.get_xticklabels())):
        if idx == 0 or idx == df["bucket"].max() - 1:
            label.set_visible(True)
        elif idx % (10 ** (spacing)) == 10 ** (spacing) - 1:
            label.set_visible(True)
        else:
            label.set_visible(False)

    plt.ylim(0, 1.5 * df[y].max())
    graph.yaxis.set_major_locator(MultipleLocator(1))

    plt.tight_layout()
    plt.savefig(figPath)

    plt.close()


def doubleLine(
    projectName: str,
    table: str,
    df: DataFrame,
    figPath: Path,
    y1: str,
    y2: str,
    yLabel: str,
) -> None:
    title: str = " ".join([word.capitalize() for word in table.split("_")])
    spacing: int = math.floor(math.log(df["bucket"].max(), 10))

    seaborn.set_theme()

    graph: Axes = seaborn.lineplot(x="bucket", data=df[[y1, y2]])
    graph.set_xlabel("Bucket")
    graph.set_ylabel(ylabel=yLabel)

    plt.title(f" {title} of {projectName}")

    idx: int
    label: Text
    for idx, label in list(enumerate(graph.get_xticklabels())):
        if idx == 0 or idx == df["bucket"].max() - 1:
            label.set_visible(True)
        elif idx % (10 ** (spacing)) == 10 ** (spacing) - 1:
            label.set_visible(True)
        else:
            label.set_visible(False)

    plt.ylim(0, 1.5 * df[y1].max())
    graph.yaxis.set_major_locator(MultipleLocator(1))

    plt.tight_layout()
    plt.savefig(figPath)

    plt.close()
