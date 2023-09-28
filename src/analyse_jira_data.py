#!/usr/bin/env python


# import pandas as pd
import configparser
import argparse
import os
import sys
import re

import jira_link
import leanStats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Jira data and get metrics")
    parser.add_argument(
        "-c",
        "--config-file",
        help="Path to config file.",
        type=str,
        required=False,
    )
    args = parser.parse_args()

    if not args.config_file:
        print("Error:  -c (config file) must be provided.")
        sys.exit(1)

    # Get configfile settings
    config = configparser.ConfigParser()
    config.read(args.config_file)
    cfg = {
        "email": config.get("JIRA", "EMAIL", fallback=None),
        "jira_url": config.get("JIRA", "JIRA_URL", fallback=None),
        "api_token": config.get("JIRA", "API_TOKEN", fallback=None),
        "project_key": config.get("JIRA", "PROJECT_KEY", fallback=None),
        "mock_jira_data": config.get("JIRA", "MOCK_JIRA_DATA", fallback=None),
        "jira_filter": config.get("JIRA", "JIRA_FILTER", fallback=None),
        "todo_names": re.split(
            r"\s*,\s*",
            config.get("BOARD", "TODO", fallback="Todo"),
        ),
        "wip_names": re.split(
            r"\s*,\s*",
            config.get("BOARD", "WIP", fallback="Doing"),
        ),
        "done_names": re.split(
            r"\s*,\s*",
            config.get("BOARD", "DONE", fallback="Done"),
        ),
        "ignore_names": re.split(
            r"\s*,\s*",
            config.get("BOARD", "IGNORE", fallback=""),
        ),
    }

    dataframe = jira_link.get_tickets(cfg)
    try:
        dataframe = leanStats.extract_ticket_timestamps(dataframe, cfg)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    dataframe = leanStats.calculate_cycletime(dataframe)
    weekly_df = leanStats.compute_metrics_per_week(dataframe)

    print_weekly_metrics(weekly_df)
