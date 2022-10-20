import os.path

from pycaiso.oasis import Node
from datetime import datetime, timedelta
import pandas as pd
import csv
import time

PATH = "CSV/"
TIMEOUT = 6


def download_LMP(name, df):
    start, end = datetime(2022, 1, 1), datetime(2022, 1, 30)
    i = TIMEOUT
    while start < datetime(2022, 9, 1):
        try:
            print("{} {}".format(name, start))
            csv_path = "{}{}-{}-{}-{}.csv".format(PATH, name, start.month, start.day, start.year)
            # If a file is already created go onto the next month
            if not os.path.exists(csv_path):
                node = Node(name)
                # create dataframe with LMPS from arbitrary period (30 day maximum)
                node_lmps = node.get_lmps(start, end)
                # drop columns that are needed for analysis
                node_df = node_lmps.drop(
                    columns=["OPR_INTERVAL", "NODE_ID_XML", "NODE", "MARKET_RUN_ID", "XML_DATA_ITEM", "PNODE_RESMRID",
                             "GRP_TYPE",
                             "POS"])
                # only get data for lmp types of LMP
                lmp = node_df[node_df["LMP_TYPE"] == "LMP"]
                lmp = lmp.drop(columns=["LMP_TYPE"])
                lmp.to_csv(csv_path, index=False)
                df = pd.concat([df, lmp])
                time.sleep(TIMEOUT)
                # reset i to the original timeout
                i = TIMEOUT
            else:
                print("File is already created")
            start += timedelta(days=30)
            end += timedelta(days=30)
        except Exception as e:
            print("Exception:\n{}".format(e))
            if str(e) == "No data available for this query.":
                # Create an empty csv file, so we dont check if the file exists
                f = open("{}{}-{}-{}-{}.csv".format(PATH, name, start.month, start.day, start.year), "w")
                f.write("File does not exist")
                f.close()
                print("Wrote empty file: {}{}-{}-{}-{}.csv".format(PATH, name, start.month, start.day, start.year))
                start += timedelta(days=30)
                end += timedelta(days=30)
                time.sleep(TIMEOUT)
            else:
                time.sleep(i)
                # cap out the the wait period to 30 seconds
                if i < 31:
                    i += 1
                print(i)
    return df


if __name__ == "__main__":
    node_df = pd.DataFrame()
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
                 "POS"])
    lmp = node_df[node_df["LMP_TYPE"] == "LMP"]
    lmp.to_csv("JAN 2022 Aug 2022 Raw_node.csv", index=False)
    file = open(PATH + "Node Prices.csv", "w+", newline="")
