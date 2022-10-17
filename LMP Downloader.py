from pycaiso.oasis import Node
from datetime import datetime, timedelta
import pandas as pd
import csv
import time

PATH = "CSV/"
TIMEOUT = 6


def readPickle(path_loc):
    try:
        df = pd.read_pickle(path_loc)
    except FileNotFoundError:
        print("Pkl file not found, creating new dataframe")
        df = pd.DataFrame()
    return df


def download_LMP(name, df):
    start, end = datetime(2022, 1, 1), datetime(2022, 1, 30)
    while start < datetime(2022, 9, 1):
        try:
            print(name)
            node = Node(name)
            # create dataframe with LMPS from arbitrary period (30 day maximum)
            node_lmps = node.get_lmps(start, end)
            df = pd.concat([df, node_lmps])
            node_df = df.drop(
                columns=["OPR_INTERVAL", "NODE_ID_XML", "NODE", "MARKET_RUN_ID", "XML_DATA_ITEM", "PNODE_RESMRID",
                         "GRP_TYPE",
                         "POS", "MW"])
            lmp = node_df[node_df["LMP_TYPE"] == "LMP"]
            lmp.to_csv("{}{}-{}-{}.csv".format(PATH, name, start.month, start.year), index=False)
            time.sleep(TIMEOUT)
        except Exception as e:
            print("Exception:\n {}".format(e))
            time.sleep(TIMEOUT)
            return df
        start += timedelta(days=30)
        end += timedelta(days=30)
    return df


if __name__ == "__main__":
    node_df = readPickle("node_df.pkl")
    # California Nodes 2018.csv
    with open("California Nodes 2018.csv", 'r') as file:
        reader = csv.reader(file)
        temp = False
        for row in reader:
            if temp:
                node_df = download_LMP(row[2], node_df)
            else:
                temp = True

    node_df = node_df.drop(
        columns=["OPR_INTERVAL", "NODE_ID_XML", "NODE", "MARKET_RUN_ID", "XML_DATA_ITEM", "PNODE_RESMRID", "GRP_TYPE",
                 "POS", "MW"])
    print(node_df)
    lmp = node_df[node_df["LMP_TYPE"] == "LMP"]
    print(lmp)
    lmp.to_pickle("node_df.pkl")
    lmp.to_csv("JAN 2019 Aug 2022 Raw_node Real.csv", index=False)
    file = open(PATH + "Node Prices.csv", "w+", newline="")
