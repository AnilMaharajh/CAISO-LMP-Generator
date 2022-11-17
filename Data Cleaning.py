import pandas as pd
import numpy as np
import os
import pytz
from datetime import date

source = os.getcwd() + "/CSV"
subdir = os.listdir(source)

node_mean = pd.DataFrame()
node_med = pd.DataFrame()
node_sd = pd.DataFrame()
node_weekday = pd.DataFrame()
node_weekend = pd.DataFrame()
node_mid = pd.DataFrame()
node_off = pd.DataFrame()
node_on = pd.DataFrame()

# based off state holidays https://www.sos.ca.gov/state-holidays
# holidays = pd.DatetimeIndex(
#     [date(2022, 1, 1), date(2022, 1, 17), date(2022, 2, 21), date(2022, 3, 31), date(2022, 5, 30),
#      date(2022, 7, 1), date(2022, 9, 5), date(2022, 11, 11), date(2022, 11, 24), date(2022, 11, 24),
#      date(2022, 12, 25), date(2022, 12, 26)])
month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
pst = pytz.timezone("US/Pacific")


def read_sub_dir(path):
    df = pd.DataFrame()
    files = os.listdir(path)
    for f in files:
        print(f)
        dir_path = "{}/{}".format(path, f)
        # An exception occurs if a csv file is empty
        try:
            t = pd.read_csv(dir_path)
            # ignore files that have "File does not exist."
            if "NODE_ID" in t.columns:
                if "LMP_TYPE" in t.columns:
                    t = t[t["LMP_TYPE"] == "LMP"]
                    # print(t)
                    # print(df)
                df = pd.concat([df, t])
        except Exception as e:
            print(e)
            # skip the empty file
    clean_data(df)


def clean_data(df):
    if "OPR_DT" in df:
        df['year'] = pd.DatetimeIndex(df['OPR_DT']).year
        # get month from the corresponding
        df['month'] = pd.DatetimeIndex(df['OPR_DT']).month
        df['weekday'] = pd.DatetimeIndex(df['OPR_DT']).dayofweek
        df['peak'] = pd.DatetimeIndex(df['OPR_DT']).hour
        # convert GMT to PST
        df['hour'] = pd.DatetimeIndex(df['INTERVALSTARTTIME_GMT'], tz=pst).hour

        week_conditions = [(df['weekday'] == 5) | (df['weekday'] == 6)]
        # 5am - 9am
        mid_peak = [(df['hour'] == 5) | (df['hour'] == 6) | (df['hour'] == 7) | (df['hour'] == 8)
                    | (df['hour'] == 9)]
        # 5pm - 9pm
        on_peak = [(df['hour'] == 17) | (df['hour'] == 18) | (df['hour'] == 19) | (df['hour'] == 20)
                   | (df['hour'] == 21)]
        # Otherwise
        off_peak = [(df['hour'] == 10) | (df['hour'] == 11) | (df['hour'] == 12) | (df['hour'] == 13)
                    | (df['hour'] == 14) | (df['hour'] == 15) | (df['hour'] == 16) | (df['hour'] == 22)
                    | (df['hour'] == 23) | (df['hour'] == 0) | (df['hour'] == 1) | (df['hour'] == 2) |
                    (df['hour'] == 3) | (df['hour'] == 4)]
        peak_time = off_peak + mid_peak + on_peak

        df["peak"] = np.select(peak_time, ['Off Peak', 'Mid Peak', 'On Peak'])
        df["weekday"] = np.select(week_conditions, ["Weekend"], default="Weekday")

        mean = df.groupby(["NODE_ID", "month", "year"]).mean(numeric_only=True)
        median = df.groupby(["NODE_ID", "month", "year"]).median(numeric_only=True)
        sd = df.groupby(["NODE_ID", "month", "year"]).std(numeric_only=True)
        mean_holi = df.groupby(["NODE_ID", "month", "year", "weekday"]).mean(numeric_only=True)
        mean_hour = df.groupby(["NODE_ID", "month", "year", "peak"]).mean(numeric_only=True)

        # index[0] Node_id Index[1] Month Index[2] Year
        for index, row in mean.iterrows():
            col_name = "{}_{}_Mean".format(month[int(index[1]) - 1], int(index[2]))
            # if column name does not exists, add it with null values
            if col_name not in node_mean:
                node_mean[col_name] = None
            node_mean.loc[index[0], col_name] = row.MW

        for index, row in median.iterrows():
            col_name = "{}_{}_Median".format(month[int(index[1]) - 1], int(index[2]))
            # if column name does not exists, add it with null values
            if col_name not in node_med:
                node_med[col_name] = None
            node_med.loc[index[0], col_name] = row.MW

        for index, row in sd.iterrows():
            col_name = "{}_{}_sd".format(month[int(index[1]) - 1], int(index[2]))
            # if column name does not exists, add it with null values
            if col_name not in node_sd:
                node_sd[col_name] = None
            node_sd.loc[index[0], col_name] = row.MW

        # index[3] week type
        for index, row in mean_holi.iterrows():
            col_name = "{}_{}_{}".format(month[int(index[1]) - 1], int(index[2]), index[3])
            # if column name does not exists, add it with null values
            if index[3] == "Weekday":
                if col_name not in node_weekday:
                    node_weekday[col_name] = None
                node_weekday.loc[index[0], col_name] = row.MW
            else:
                if col_name not in node_weekday:
                    node_weekend[col_name] = None
                node_weekend.loc[index[0], col_name] = row.MW

        # index[3] peak
        for index, row in mean_hour.iterrows():
            col_name = "{}_{}_{}".format(month[int(index[1]) - 1], int(index[2]), index[3])
            # if column name does not exists, add it with null values
            if index[3] == "Mid Peak":
                if col_name not in node_mid:
                    node_mid[col_name] = None
                node_mid.loc[index[0], col_name] = row.MW
            elif index[3] == "Off Peak":
                if col_name not in node_off:
                    node_off[col_name] = None
                node_off.loc[index[0], col_name] = row.MW
            else:
                if col_name not in node_on:
                    node_on[col_name] = None
                node_on.loc[index[0], col_name] = row.MW
    return df


if __name__ == "__main__":
    for sdir in subdir:
        read_sub_dir("{}/{}".format(source, sdir))

    node_mean.to_csv("Nodes_Mean.csv")
    node_med.to_csv("Nodes_Median.csv")
    node_sd.to_csv("Nodes_Standard_Deviation.csv")
    node_weekday.to_csv("Nodes Weekday.csv")
    node_weekend.to_csv("Nodes Weekend.csv")
    node_mid.to_csv("Nodes Mid.csv")
    node_off.to_csv("Nodes Off.csv")
    node_on.to_csv("Nodes On.csv")
