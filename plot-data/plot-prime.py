# imports
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Path to .db file
sql_file_path = "prime.db"

def plot_table(tableName,x,y):
    # Connect to the SQLite database
    conn = sqlite3.Connection(database=sql_file_path)
    df = pd.read_sql_query("SELECT * FROM "+tableName, con=conn)
    conn.close()
    
    print(df)

    # plot stats
    sns.set_theme()
    sns.relplot(kind="line", x=x, y=y, data=df)
    plt.title(x+" and "+y)
    plt.tight_layout()
    plt.savefig(tableName+".png")

plot_table("daily_bus_factor","bucket","bus_factor")