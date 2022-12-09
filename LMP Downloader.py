import os.path

from pycaiso.oasis import Node
from datetime import datetime, timedelta
import pandas as pd
import csv
import time

PATH = "CSV/"
TIMEOUT = 6


def download_LMP(name, df):
    start, end, final_date = datetime(2019, 6, 30), datetime(2019, 7, 30), datetime(2020, 1, 1)

    # Check to see if a folder is already created for the node
    folder_path = "{}{}/".format(PATH, name)
    if not os.path.exists(folder_path):
        print("Create folder for {}".format(name))
        os.mkdir(folder_path)

    while start < final_date:
        # If the end of the interval exceeds the end, final date, make the end the same
        if end > final_date:
            end = final_date
        try:
            print("{} {}".format(name, start))
            csv_path = "{}{}-{}-{}-{}.csv".format(folder_path, name, start.month, start.day, start.year)
            # If a file is already created go onto the next month
            if not os.path.exists(csv_path):
                node = Node(name)
                # create dataframe with LMPS from arbitrary period (30 day maximum)
                node_lmps = node.get_lmps(start, end)
                # drop columns that are not needed for analysis
                node_df = node_lmps.drop(
                    columns=["OPR_INTERVAL", "NODE_ID_XML", "NODE", "MARKET_RUN_ID", "XML_DATA_ITEM", "PNODE_RESMRID",
                             "GRP_TYPE",
                             "POS"])
                # only get data for lmp types of LMP
                node_df.to_csv(csv_path, index=False)
                node_df = node_df[node_df["LMP_TYPE"] == "LMP"]
                df = pd.concat([df, node_df])
                time.sleep(TIMEOUT)
                # reset i to the original timeout
            else:
                print("File is already created")
            start += timedelta(days=30)
            end += timedelta(days=30)
        except Exception as e:
            print("Exception:\n{}".format(e))
            if str(e) == "No data available for this query.":
                # Create an empty csv file, so we do not check for it again
                f = open("{}{}-{}-{}-{}.csv".format(folder_path, name, start.month, start.day, start.year), "w")
                f.write("File does not exist")
                f.close()
                print("Wrote empty file: {}{}-{}-{}-{}.csv".format(folder_path, name, start.month, start.day, start.year))
                start += timedelta(days=30)
                end += timedelta(days=30)
                time.sleep(TIMEOUT)
            else:
                time.sleep(TIMEOUT)
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
    # write all the data into one big csv
    node_df.to_csv("2021 Raw_node.csv", index=False)
