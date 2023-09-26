#!/usr/bin/env python3

import argparse
import configparser
import pandas as pd
import numpy as np

# from pprint import pprint
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


def compute_metrics(dataframe, lookback_window):
    # Sort dataframe by 'timestamp_end'
    dataframe = dataframe.sort_values(by="timestamp_end")

    # Initialize new columns
    dataframe["median_cycletime"] = np.nan
    dataframe["p85_cycletime"] = np.nan
    dataframe["throughput"] = np.nan

    # For each ticket, compute metrics over a 7-day lookback window
    for idx, row in dataframe.iterrows():
        lookback_start = row["timestamp_end"] - pd.Timedelta(days=lookback_window)
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
    parser.add_argument(
        "-w",
        "--lookback-window",
        help="Size of window in days for metric calculations. (default 7)",
        type=int,
        default=7,
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
        try:
            lookback_window = int(config.get("METRICS", "lookback_window", fallback=7))
        except ValueError:
            print(f"Config file error: lookback_window needs to be a number.")
            sys.exit(1)
    else:
        file_path = args.input_csv_file
        lookback_window = args.lookback_window

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
    dataframe = compute_metrics(dataframe, lookback_window)

    # select which fields I want
    dataframe = dataframe[
        [
            "Key",
            "timestamp_start",
            "timestamp_end",
            "cycletime",
            "median_cycletime",
            "p85_cycletime",
            "throughput",
        ]
    ]

    # Print to stdout
    def format_value(value):
        s = str(value)
        return s.rstrip(".0") if "." in s else s

    tickets_data = dataframe.sort_values(by="timestamp_end").values.tolist()
    headers = [
        "Key",
        "timestamp_start",
        "timestamp_end",
        "cycletime",
        "median_cycletime",
        "p85_cycletime",
        "throughput",
    ]
    print("|".join(headers))
    for row in tickets_data:
        print("|".join(map(format_value, row)))


if __name__ == "__main__":
    main()
