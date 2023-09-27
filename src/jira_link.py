#!/usr/bin/env python

from jira import JIRA
import pandas as pd
import os
import sys


def connect_to_jira(cfg):
    options = {"server": cfg["jira_url"]}
    return JIRA(options, basic_auth=(cfg["email"], cfg["api_token"]))


def get_tickets(cfg):
    if cfg["mock_jira_data"]:
        return get_tickets_from_mockfile(cfg)
    else:
        jira_client = connect_to_jira(cfg)
        return get_tickets_from_jira(jira_client, cfg)


def get_tickets_from_jira(jira_client, cfg):
    filter_name = cfg["jira_filter"]
    saved_filters = jira_client.favourite_filters()
    target_filter = next((f for f in saved_filters if f.name == filter_name), None)

    if not target_filter:
        raise ValueError(f"Jira filter named '{filter_name}' not found.")

    jql_str = target_filter.jql

    issues = jira_client.search_issues(
        jql_str=jql_str, expand="changelog", maxResults=10
    )

    ticket_data = []

    for issue in issues:
        changelog = issue.changelog
        for history in changelog.histories:
            for item in history.items:
                if item.field == "status":
                    ticket_data.append(
                        {
                            "ticket_id": issue.key,
                            "from_status": item.fromString,
                            "to_status": item.toString,
                            "changed_at": history.created,
                        }
                    )

    return pd.DataFrame(ticket_data)


def get_tickets_from_mockfile(cfg):
    mock_datafile = cfg["mock_jira_data"]
    if not os.path.isfile(mock_datafile):
        print(
            f"The mock jira data file '{mock_datafile}' does not exist or is not readable."
        )
        sys.exit(1)

    return pd.read_csv(file_path, parse_dates=["changed_at"])


def print_help():
    print("jira_link.py - get ticket details from Jira")


if __name__ == "__main__":
    import configparser
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Jira ticket data")
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
    }

    # connect to a source and get ticket data
    # source can be: jira or mockfile
    dataframe = get_tickets(cfg)

    print(dataframe.sort_values(by="changed_at").to_string(index=False))
