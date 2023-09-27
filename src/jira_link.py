#!/usr/bin/env python

from jira import JIRA
import pandas as pd


def connect_to_jira(jira_url, api_key, user_email):
    options = {"server": jira_url}
    return JIRA(options, basic_auth=(user_email, api_key))


def get_tickets_as_dataframe(jira_client, jira_filter):
    issues = jira_client.search_issues(
        jql_str=jira_filter, expand="changelog", maxResults=100
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
    email = config.get("JIRA", "EMAIL", fallback=None)
    api_token = config.get("JIRA", "API_TOKEN", fallback=None)
    jira_instance = config.get("JIRA", "JIRA_URL", fallback=None)
    project_key = config.get("JIRA", "PROJECT_KEY", fallback=None)

    # connect to jira and get ticket data
    jira_client = connect_to_jira(jira_instance, api_token, email)
    dataframe = get_tickets_as_dataframe(
        jira_client,
        (
            f"project = '{project_key}' AND "
            "issuetype in (Bug, Story, Task) AND "
            'status changed DURING ("2023/09/11", "2023/09/24")'
        ),
    )

    print(dataframe.sort_values(by="changed_at").to_string(index=False))
