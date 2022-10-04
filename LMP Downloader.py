from pycaiso.oasis import Node
from datetime import datetime
import pandas as pd
import csv
import time

path = "CSV/"


def readPickle(path_loc):
    try:
        df = pd.read_pickle(path_loc)
    except FileNotFoundError:
        print("Pkl file not found, creating new dataframe")
        df = pd.DataFrame()
    return df


def download_LMP(name, df):
    print(name)
    data = []
    node = Node(name)
    # create dataframe with LMPS from arbitrary period (30 day maximum)
    node_lmps = node.get_lmps(datetime(2022, 9, 1), datetime(2022, 9, 30))
    df = pd.concat([df, node_lmps])
    print(node_lmps)
    mean = node_lmps.groupby(["MW"]).mean()
    print(mean)
    data.append([name, mean])
    time.sleep(3)
    return df, data


if __name__ == "__main__":
    node_df = readPickle("node_df.pkl")
    # California Nodes 2018.csv
    with open("temp.csv", 'r') as file:
        reader = csv.reader(file)
        temp = False
        for row in reader:
            if temp:
                node_df, data = download_LMP(row[2], node_df)
            else:
                temp = True
    node_df.to_pickle("node_df.pkl")
    node_df.to_csv("Raw_node.csv", index=False)
    file = open(path + "Node Prices.csv", "w+", newline="")
    with file:
        write = csv.writer(file)
        write.writerows(data)
