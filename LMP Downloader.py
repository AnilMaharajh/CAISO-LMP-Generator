from pycaiso.oasis import Node
from datetime import datetime
import pandas as pd
import csv
import time

path = "CSV/"
data = []

node_df = pd.DataFrame()
name = "ALPINE_6_N001"
cj = Node(name)
# create dataframe with LMPS from arbitrary period (30 day maximum)
time.sleep(6)
cj_lmps = cj.get_lmps(datetime(2022, 9, 1), datetime(2022, 9, 30))
print(name)
print(cj_lmps)
data.append([name, cj_lmps["MW"].mean])
print(cj_lmps["MW"].mean)
node_df.to_pickle("node_df.pkl")


# with open("California Nodes 2018.csv", 'r') as file:
#     reader = csv.reader(file)
#     temp = False
#     for row in reader:
#         if temp:
#             name = row[2]
#             print(name)
#             cj = Node(name)
#             # create dataframe with LMPS from arbitrary period (30 day maximum)
#             try:
#                 cj_lmps = cj.get_lmps(datetime(2022, 9, 21), datetime(2022, 9, 22))
#                 data.append([name, cj_lmps["MW"].mean])
#                 time.sleep(6)
#             except:
#                 print("{} has no data".format(name))
#         else:
#             temp = True

file = open(path + "Node Prices.csv", "w+", newline="")
with file:
    write = csv.writer(file)
    write.writerows(data)
