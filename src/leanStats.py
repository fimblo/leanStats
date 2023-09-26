#!/usr/bin/env python3

import argparse
import configparser
import pandas as pd
import numpy as np

from pprint import pprint
import sys
import os


def extract_ticket_timestamps(file_path):
    data = pd.read_csv(
        file_path, parse_dates=["Status Transition.date"], dayfirst=True
    )  # dd/mm/yy madness

    # filter when tickets moved to IN PROGRESS
    in_progress = (
        data[data["Status Transition.to"].str.upper() == "IN PROGRESS"]
        .groupby("Key")
        .agg({"Status Transition.date": "min"})
        .rename(columns={"Status Transition.date": "timestamp_start"})
    )

    # filter when tickets moved to DONE
    done = (
        data[data["Status Transition.to"].str.upper() == "DONE"]
        .groupby("Key")
        .agg({"Status Transition.date": "max"})
        .rename(columns={"Status Transition.date": "timestamp_end"})
    )

    # left join on key.
    return in_progress.join(done, on="Key").reset_index()


def calculate_cycletime(dataframe):
    df_copy = dataframe.copy()

    time_difference = df_copy["timestamp_end"] - df_copy["timestamp_start"]
    total_seconds = time_difference.dt.total_seconds()
    cycletime_in_days = np.ceil(total_seconds / (24 * 3600))
    df_copy["cycletime"] = cycletime_in_days.astype(int)

    return df_copy


def compute_metrics(dataframe):
    # Sort dataframe by 'timestamp_end'
    dataframe = dataframe.sort_values(by="timestamp_end")

    # Initialize new columns
    dataframe["median_cycletime"] = np.nan
    dataframe["p85_cycletime"] = np.nan
    dataframe["throughput"] = np.nan

    # For each ticket, compute metrics over lookback window of 1 week
    for idx, row in dataframe.iterrows():
        lookback_start = row["timestamp_end"] - pd.Timedelta(days=7)
        lookback_end = row["timestamp_end"]

        # Filter dataframe for tickets within the lookback window
        lookback_data = dataframe[
            (dataframe["timestamp_end"] >= lookback_start)
            & (dataframe["timestamp_end"] <= lookback_end)
        ]

        # Compute metrics
        dataframe.at[idx, "median_cycletime"] = np.ceil(
            lookback_data["cycletime"].median()
        )
        dataframe.at[idx, "p85_cycletime"] = np.ceil(
            lookback_data["cycletime"].quantile(0.85)
        )
        dataframe.at[idx, "throughput"] = np.ceil(lookback_data.shape[0])

    return dataframe


def weekly_metrics(dataframe):
    # populate week column
    dataframe["week"] = dataframe["timestamp_end"].dt.strftime("%Y-W%U")

    # Group by week
    weekly_data = (
        dataframe.groupby("week")
        .agg(
            {
                "cycletime": [
                    lambda x: np.ceil(x.quantile(0.5)),
                    lambda x: np.ceil(x.quantile(0.85)),
                ],
                "Key": "count",  # throughput
            }
        )
        .reset_index()
    )
    weekly_data.columns = ["week", "cycletime_p50", "cycletime_p85", "throughput"]

    # calculate start and end dates for all weeks
    weekly_data["startdate"] = pd.to_datetime(
        weekly_data["week"].apply(lambda x: x + "-1"), format="%Y-W%U-%w"
    )
    weekly_data["enddate"] = weekly_data["startdate"] + pd.Timedelta(days=6)

    # Clone the date data from weekly_data to all_weeks
    all_weeks = weekly_data[["week", "startdate", "enddate"]].copy()

    # Generate a dataframe with all weeks in the range
    complete_weeks = pd.DataFrame(
        {
            "startdate": pd.date_range(
                start=weekly_data["startdate"].min(),
                end=weekly_data["enddate"].max(),
                freq="W-MON",
            )
        }
    )
    complete_weeks["enddate"] = complete_weeks["startdate"] + pd.Timedelta(days=6)
    complete_weeks["week"] = complete_weeks["startdate"].dt.strftime("%Y-W%U")

    # Merge complete_weeks with all_weeks to fill in missing weeks
    all_weeks = pd.merge(
        complete_weeks, all_weeks, on=["week", "startdate", "enddate"], how="left"
    )

    # Now merge all_weeks with weekly_data to get the metrics
    result = pd.merge(
        all_weeks,
        weekly_data.drop(columns=["startdate", "enddate"]),
        on="week",
        how="left",
    )

    return result[
        ["startdate", "enddate", "cycletime_p50", "cycletime_p85", "throughput"]
    ]


def print_weekly_metrics(dataframe_in):
    print(dataframe_in.to_string(index=False))


def print_ticket_metrics(dataframe_in):
    print(dataframe_in.sort_values(by="timestamp_end").to_string(index=False))


def print_help():
    print("leanStats.py - get lean metrics from jira csv")


def main():
    parser = argparse.ArgumentParser(description="calculate lean metrics")
    parser.add_argument(
        "-c",
        "--config-file",
        help="Path to config file. Overrides all other commandline params if specified.",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-i",
        "--input-csv-file",
        help="Path to input file with CSV values.",
        type=str,
        required=False,
    )
    args = parser.parse_args()

    # If a configuration file is provided, overwrite all command-line
    # arguments
    if args.config_file:
        if not os.path.isfile(args.config_file):
            print(f"The file '{args.config_file}' does not exist or is not readable.")
            sys.exit(1)
        config = configparser.ConfigParser()
        config.read(args.config_file)

        file_path = config.get("SYSTEM", "input_csv_file", fallback=None)
    else:
        file_path = args.input_csv_file

    # Validate command line arguments
    if not args.config_file and not file_path:
        print("Error: Either -c (config file) or -i (input file) must be provided.")
        sys.exit(1)

    # Sanity checks
    if not os.path.isfile(file_path):
        print(f"The file '{file_path}' does not exist or is not readable.")
        sys.exit(1)

    # read in data and calculate cycletime
    dataframe = extract_ticket_timestamps(file_path)
    dataframe = calculate_cycletime(dataframe)
    dataframe = compute_metrics(dataframe)

    print_ticket_metrics(dataframe)

    # get weekly stats
    weekly_df = weekly_metrics(dataframe)
    print_weekly_metrics(weekly_df)


if __name__ == "__main__":
    main()
