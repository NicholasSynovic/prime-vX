import math
from typing import Path

import matplotlib.pyplot as plt
import seaborn
from matplotlib.axes import Axes
from matplotlib.text import Text
from matplotlib.ticker import MultipleLocator
from pandas import DataFrame


def plot(projectName: str, table: str, df: DataFrame, figPath: Path) -> None:
    title: str = " ".join([word.capitalize() for word in table.split("_")])
    spacing: int = math.floor(math.log(df["bucket"].max(), 10))

    seaborn.set_theme()

    graph: Axes = seaborn.barplot(x="bucket", y="bus_factor", data=df)
    graph.set_xlabel("Bucket")
    graph.set_ylabel("Bus Factor")

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

    plt.ylim(0, 1.5 * df["bus_factor"].max())
    graph.yaxis.set_major_locator(MultipleLocator(1))

    plt.tight_layout()
    plt.savefig(figPath)

    plt.close()
