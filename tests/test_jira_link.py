#!/usr/bin/env python

import pandas as pd
from unittest.mock import patch, Mock
from jira_link import connect_to_jira, get_tickets_as_dataframe


def test_connect_to_jira():
    # replace JIRA class inside the jira_connector module temporarily
    # with a mock object.
    with patch("jira_link.JIRA") as mock_jira:
        mock_instance = Mock()
        mock_jira.return_value = mock_instance

        result = connect_to_jira(
            "https://jira.example.com", "api_key_123", "email@example.com"
        )

        mock_jira.assert_called_once_with(
            {"server": "https://jira.example.com"},
            basic_auth=("email@example.com", "api_key_123"),
        )
        assert result == mock_instance


@patch("jira_link.JIRA")
def test_get_tickets_as_dataframe(mocked_jira_class):
    # Given: a mock jira client
    mock_item = Mock()
    mock_item.field = "status"
    mock_item.fromString = "Open"
    mock_item.toString = "Closed"

    mock_history = Mock()
    mock_history.items = [mock_item]
    mock_history.created = "2023-09-26T15:59:30.846+0200"

    mock_issue = Mock()
    mock_issue.key = "TEST-123"
    mock_issue.changelog.histories = [mock_history]

    mocked_jira_client = mocked_jira_class.return_value
    mocked_jira_client.search_issues.return_value = [mock_issue]

    # When: Execute the behavior under test (note: no need for a real
    # filter, since the mocked_jira_client returns what it does
    # regardless of the filter)
    df = get_tickets_as_dataframe(mocked_jira_client, "some_filter")

    # Then: the dataframe we get above should match the expected result
    expected_df = pd.DataFrame(
        {
            "ticket_id": ["TEST-123"],
            "from_status": ["Open"],
            "to_status": ["Closed"],
            "changed_at": ["2023-09-26T15:59:30.846+0200"],
        }
    )

    pd.testing.assert_frame_equal(df, expected_df)
