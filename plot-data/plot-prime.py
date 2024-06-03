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

def plot_table(tableName,x,y):
    # Connect to the SQLite database
    conn = sqlite3.Connection(database=sql_file_path)
    df = pd.read_sql_query("SELECT * FROM "+tableName, con=conn)
    conn.close()

    # plot stats
    sns.set_theme()
    graph = sns.barplot(x=x, y=y, data=df)

    graph.set_xlabel("Bucket")
    graph.set_ylabel("Bus Factor")
    plt.title("Annual Bus Factor of "+projectName)

    spacing = math.floor(math.log(df[x].max(),10))
    print (tableName+str(spacing))
    print ("xmax: "+str(df[x].max()))

    for ind, label in enumerate(graph.get_xticklabels()):
        if ind % (10**(spacing)) == 10**(spacing)-1:
            label.set_visible(True)
        else:
            label.set_visible(False)

    #Set x and y axes
    max_value = df[y].max()
    plt.ylim(0, 2 * max_value)
    graph.yaxis.set_major_locator(MultipleLocator(1))

    plt.tight_layout()
    plt.savefig(tableName+".png")

    plt.close()


plot_table("annual_bus_factor","bucket","bus_factor")
plot_table("six_month_bus_factor","bucket","bus_factor")
plot_table("three_month_bus_factor","bucket","bus_factor")
plot_table("two_month_bus_factor","bucket","bus_factor")
plot_table("monthly_bus_factor","bucket","bus_factor")