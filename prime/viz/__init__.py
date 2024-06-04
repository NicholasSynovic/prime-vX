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
) -> None:
    # Connect to the SQLite database
    conn = sqlite3.Connection(database=sql_file_path)
    df = pd.read_sql_query("SELECT * FROM " + tableName, con=conn)
    conn.close()

    # plot stats
    sns.set_theme()
    graph = sns.lineplot(data=df[["effort_kloc", "productivity_kloc"]], estimator=None)
    # sns.relplot(kind="line", x=x, y="productivity_kloc", data=df,estimator=None)

    words = tableName.split("_")
    capitalized_words = [word.capitalize() for word in words]

    graph.set(
        xlabel="Bucket",
        ylabel="Productivity",
        title=(" ".join(capitalized_words) + " of " + projectName),
    )

    plt.title(" ".join(capitalized_words) + " of " + projectName)

    spacing = math.floor(math.log(df[x].max(), 10))

    # for ind, label in list(enumerate(graph.get_xticklabels())):
    #     if ind % (10**(spacing)) == 10**(spacing)-1:
    #         label.set_visible(True)
    #     else:
    #         label.set_visible(False)

    #     if ind == 0 or ind == df[x].max()-1:
    #         label.set_visible(True)

    # Set x and y axes
    max_value = df[y].max()
    # plt.ylim(0, 2 * max_value)
    # graph.yaxis.set_major_locator(MultipleLocator(1))

    plt.tight_layout()
    plt.savefig(tableName + ".png")

    plt.close()


plot_productivity("annual_productivity", "bucket", "effort_kloc")
plot_productivity("six_month_productivity", "bucket", "effort_kloc")
plot_productivity("three_month_productivity", "bucket", "effort_kloc")
