# imports
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.ticker import MultipleLocator
import math

# Path to .db file
sql_file_path = "prime.db"
projectName="My Project"

def plot_bus_factor(tableName,x,y):
    # Connect to the SQLite database
    conn = sqlite3.Connection(database=sql_file_path)
    df = pd.read_sql_query("SELECT * FROM "+tableName, con=conn)
    conn.close()

    # plot stats
    sns.set_theme()
    graph = sns.barplot(x=x, y=y, data=df)

    graph.set_xlabel("Bucket")
    graph.set_ylabel("Bus Factor")

    words = tableName.split('_')
    capitalized_words = [word.capitalize() for word in words]

    plt.title(' '.join(capitalized_words)+" of "+projectName)

    spacing = math.floor(math.log(df[x].max(),10))

    for ind, label in list(enumerate(graph.get_xticklabels())):
        if ind % (10**(spacing)) == 10**(spacing)-1:
            label.set_visible(True)
        else:
            label.set_visible(False)
            
        if ind == 0 or ind == df[x].max()-1:
            label.set_visible(True)
            

    #Set x and y axes
    max_value = df[y].max()
    plt.ylim(0, 2 * max_value)
    graph.yaxis.set_major_locator(MultipleLocator(1))

    plt.tight_layout()
    plt.savefig(tableName+".png")

    plt.close()


plot_bus_factor("annual_bus_factor","bucket","bus_factor")
plot_bus_factor("six_month_bus_factor","bucket","bus_factor")
plot_bus_factor("three_month_bus_factor","bucket","bus_factor")
plot_bus_factor("two_month_bus_factor","bucket","bus_factor")
plot_bus_factor("monthly_bus_factor","bucket","bus_factor")
plot_bus_factor("two_week_bus_factor","bucket","bus_factor")
plot_bus_factor("weekly_bus_factor","bucket","bus_factor")