# imports
import math
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.ticker import MultipleLocator

# Path to .db file
sql_file_path = "prime_0.1.1.db"
projectName = "My Project"


def plot_bus_factor(tableName, x, y):
    # Connect to the SQLite database
    conn = sqlite3.Connection(database=sql_file_path)
    df = pd.read_sql_query("SELECT * FROM " + tableName, con=conn)
    conn.close()

    # plot stats
    sns.set_theme()
    graph = sns.barplot(x=x, y=y, data=df)

    graph.set_xlabel("Bucket")
    graph.set_ylabel("Bus Factor")

    words = tableName.split("_")
    capitalized_words = [word.capitalize() for word in words]

    plt.title(" ".join(capitalized_words) + " of " + projectName)

    spacing = math.floor(math.log(df[x].max(), 10))

    for ind, label in list(enumerate(graph.get_xticklabels())):
        if ind % (10 ** (spacing)) == 10 ** (spacing) - 1:
            label.set_visible(True)
        else:
            label.set_visible(False)

        if ind == 0 or ind == df[x].max() - 1:
            label.set_visible(True)

    # Set x and y axes
    max_value = df[y].max()
    plt.ylim(0, 2 * max_value)
    graph.yaxis.set_major_locator(MultipleLocator(1))

    plt.tight_layout()
    plt.savefig(tableName + ".png")

    plt.close()


# plot_bus_factor("annual_bus_factor","bucket","bus_factor")
# plot_bus_factor("six_month_bus_factor","bucket","bus_factor")
# plot_bus_factor("three_month_bus_factor","bucket","bus_factor")
# plot_bus_factor("two_month_bus_factor","bucket","bus_factor")
# plot_bus_factor("monthly_bus_factor","bucket","bus_factor")
# plot_bus_factor("two_week_bus_factor","bucket","bus_factor")
# plot_bus_factor("weekly_bus_factor","bucket","bus_factor")


def plot_developer_count(tableName, x, y):
    # Connect to the SQLite database
    conn = sqlite3.Connection(database=sql_file_path)
    df = pd.read_sql_query("SELECT * FROM " + tableName, con=conn)
    conn.close()

    # plot stats
    sns.set_theme()
    graph = sns.barplot(x=x, y=y, data=df)

    graph.set_xlabel("Bucket")
    graph.set_ylabel("Developer Count")

    words = tableName.split("_")
    capitalized_words = [word.capitalize() for word in words]

    plt.title(" ".join(capitalized_words) + " of " + projectName)

    spacing = math.floor(math.log(df[x].max(), 10))

    for ind, label in list(enumerate(graph.get_xticklabels())):
        if ind % (10 ** (spacing)) == 10 ** (spacing) - 1:
            label.set_visible(True)
        else:
            label.set_visible(False)

        if ind == 0 or ind == df[x].max() - 1:
            label.set_visible(True)

    # Set x and y axes
    max_value = df[y].max()
    plt.ylim(0, 2 * max_value)
    graph.yaxis.set_major_locator(MultipleLocator(1))

    plt.tight_layout()
    plt.savefig(tableName + ".png")

    plt.close()


# plot_developer_count("annual_developer_count","bucket","developer_count")
# plot_developer_count("six_month_developer_count","bucket","developer_count")
# plot_developer_count("three_month_developer_count","bucket","developer_count")
# plot_developer_count("two_month_developer_count","bucket","developer_count")
# plot_developer_count("monthly_developer_count","bucket","developer_count")
# plot_developer_count("two_week_developer_count","bucket","developer_count")
# plot_developer_count("weekly_developer_count","bucket","developer_count")


def plot_productivity(tableName, x, y):
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
# plot_developer_count("two_month_developer_count","bucket","developer_count")
# plot_developer_count("monthly_developer_count","bucket","developer_count")
# plot_developer_count("two_week_developer_count","bucket","developer_count")
# plot_developer_count("weekly_developer_count","bucket","developer_count")
