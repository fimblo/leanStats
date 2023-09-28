import pytest
import pandas as pd
import numpy as np
from io import StringIO
import sys
import os

from leanStats import (
    extract_ticket_timestamps,
    calculate_cycletime,
    compute_metrics_per_ticket,
    compute_metrics_per_week,
    check_statuses_defined,
)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def sample_data_time():
    data = """ticket_id,changed_at,to_status
TICKET-1,15/09/2023 00:01:00,IN PROGRESS
TICKET-1,17/09/2023 00:02:00,DONE
TICKET-2,16/09/2023 00:01:00,IN PROGRESS
TICKET-2,20/09/2023 00:02:00,DONE
"""
    return StringIO(data)


@pytest.fixture
def sample_data_date():
    data = """ticket_id,changed_at,to_status
TICKET-1,15/09/2023,IN PROGRESS
TICKET-1,17/09/2023,DONE
TICKET-2,16/09/2023,IN PROGRESS
TICKET-2,20/09/2023,DONE
"""
    return StringIO(data)


def test_extract_ticket_datestamps(sample_data_date):
    data = pd.read_csv(sample_data_date, parse_dates=["changed_at"], dayfirst=True)
    cfg = {
        "todo_names": ["TODO"],
        "wip_names": ["IN PROGRESS"],
        "done_names": ["DONE"],
    }
    df = extract_ticket_timestamps(data, cfg)

    assert len(df) == 2
    assert df.iloc[0]["ticket_id"] == "TICKET-1"
    assert df.iloc[0]["timestamp_start"].strftime("%Y-%m-%d") == "2023-09-15"
    assert df.iloc[0]["timestamp_end"].strftime("%Y-%m-%d") == "2023-09-17"
    assert df.iloc[1]["ticket_id"] == "TICKET-2"
    assert df.iloc[1]["timestamp_start"].strftime("%Y-%m-%d") == "2023-09-16"
    assert df.iloc[1]["timestamp_end"].strftime("%Y-%m-%d") == "2023-09-20"


def test_extract_ticket_timestamps(sample_data_time):
    data = pd.read_csv(sample_data_time, parse_dates=["changed_at"], dayfirst=True)
    cfg = {
        "todo_names": ["TODO"],
        "wip_names": ["IN PROGRESS"],
        "done_names": ["DONE"],
    }
    df = extract_ticket_timestamps(data, cfg)

    assert len(df) == 2
    assert df.iloc[0]["ticket_id"] == "TICKET-1"
    assert (
        df.iloc[0]["timestamp_start"].strftime("%Y-%m-%d %H:%M:%S")
        == "2023-09-15 00:01:00"
    )
    assert (
        df.iloc[0]["timestamp_end"].strftime("%Y-%m-%d %H:%M:%S")
        == "2023-09-17 00:02:00"
    )
    assert df.iloc[1]["ticket_id"] == "TICKET-2"
    assert (
        df.iloc[1]["timestamp_start"].strftime("%Y-%m-%d %H:%M:%S")
        == "2023-09-16 00:01:00"
    )
    assert (
        df.iloc[1]["timestamp_end"].strftime("%Y-%m-%d %H:%M:%S")
        == "2023-09-20 00:02:00"
    )


def test_calculate_cycletime(sample_data_time):
    data = pd.read_csv(sample_data_time, parse_dates=["changed_at"], dayfirst=True)
    cfg = {
        "todo_names": ["TODO"],
        "wip_names": ["IN PROGRESS"],
        "done_names": ["DONE"],
    }
    df = extract_ticket_timestamps(data, cfg)
    df = calculate_cycletime(df)

    # we always round cycle time up.
    assert len(df) == 2
    assert (
        df.iloc[0]["cycletime"] == 3
    )  # 3 days from '2023-09-15' to '2023-09-17 + 1 minute'
    assert (
        df.iloc[1]["cycletime"] == 5
    )  # 5 days from '2023-09-16' to '2023-09-20 + 1 minute'


def test_compute_metrics_per_ticket_basic_functionality():
    # Given: Create a mock dataframe
    data = {
        "ticket_id": ["A", "B", "C", "D"],
        "timestamp_end": [
            pd.Timestamp("2023-09-15 10:00:00"),
            pd.Timestamp("2023-09-17 10:00:00"),
            pd.Timestamp("2023-09-19 10:00:00"),
            pd.Timestamp("2023-09-20 10:00:00"),
        ],
        "cycletime": [5, 3, 4, 2],
    }
    df = pd.DataFrame(data)

    # When: Run the compute_metrics_per_ticket function
    result_df = compute_metrics_per_ticket(df)

    # Then: Check the basic functionality

    # Check columns are present
    assert "median_cycletime" in result_df.columns
    assert "p85_cycletime" in result_df.columns
    assert "throughput" in result_df.columns

    # Check correct calculations for each ticket
    assert (
        result_df.loc[result_df["ticket_id"] == "A", "median_cycletime"].values[0] == 5
    )
    assert result_df.loc[result_df["ticket_id"] == "A", "p85_cycletime"].values[0] == 5
    assert result_df.loc[result_df["ticket_id"] == "A", "throughput"].values[0] == 1

    assert (
        result_df.loc[result_df["ticket_id"] == "B", "median_cycletime"].values[0] == 4
    )
    assert result_df.loc[result_df["ticket_id"] == "B", "p85_cycletime"].values[0] == 5
    assert result_df.loc[result_df["ticket_id"] == "B", "throughput"].values[0] == 2

    assert (
        result_df.loc[result_df["ticket_id"] == "C", "median_cycletime"].values[0] == 4
    )
    assert result_df.loc[result_df["ticket_id"] == "C", "p85_cycletime"].values[0] == 5
    assert result_df.loc[result_df["ticket_id"] == "C", "throughput"].values[0] == 3

    assert (
        result_df.loc[result_df["ticket_id"] == "D", "median_cycletime"].values[0] == 4
    )
    assert result_df.loc[result_df["ticket_id"] == "D", "p85_cycletime"].values[0] == 5
    assert result_df.loc[result_df["ticket_id"] == "D", "throughput"].values[0] == 4


def test_compute_metrics_per_week_basic_input():
    # Given: A basic input DataFrame
    data = {
        "timestamp_end": ["2023-01-01", "2023-01-03", "2023-01-05"],
        "cycletime": [5, 7, 6],
        "ticket_id": [1, 2, 3],
    }
    df = pd.DataFrame(data)
    df["timestamp_end"] = pd.to_datetime(df["timestamp_end"])

    # When: Calling the compute_metrics_per_week function
    result = compute_metrics_per_week(df)

    # Then: It should handle and return the expected output
    expected = [(pd.Timestamp("2023-01-02"), pd.Timestamp("2023-01-08"), 6.0, 7.0, 3)]
    assert list(result.itertuples(index=False, name=None)) == expected


def test_compute_metrics_per_week_fill_missing_week():
    # Given: Input data with a gap week
    data = {
        "timestamp_end": ["2023-01-01", "2023-01-15"],
        "cycletime": [5, 7],
        "ticket_id": [1, 2],
    }
    df = pd.DataFrame(data)
    df["timestamp_end"] = pd.to_datetime(df["timestamp_end"])

    # When: Calling the compute_metrics_per_week function
    result = compute_metrics_per_week(df)

    # Then: It should fill the missing week with NaN
    expected = [
        (pd.Timestamp("2023-01-02"), pd.Timestamp("2023-01-08"), 5.0, 5.0, 1),
        (
            pd.Timestamp("2023-01-09"),
            pd.Timestamp("2023-01-15"),
            np.nan,
            np.nan,
            np.nan,
        ),
        (pd.Timestamp("2023-01-16"), pd.Timestamp("2023-01-22"), 7.0, 7.0, 1),
    ]

    expected_df = pd.DataFrame(
        expected,
        columns=[
            "startdate",
            "enddate",
            "cycletime_p50",
            "cycletime_p85",
            "throughput",
        ],
    )

    # Use the pandas equals method which considers NaN values as equal
    assert result.equals(expected_df)


def test_compute_metrics_per_week_expected_columns():
    # Given: Any DataFrame
    data = {
        "timestamp_end": ["2023-01-01", "2023-01-03"],
        "cycletime": [5, 6],
        "ticket_id": [1, 2],
    }
    df = pd.DataFrame(data)
    df["timestamp_end"] = pd.to_datetime(df["timestamp_end"])

    # When: Calling the compute_metrics_per_week function
    result = compute_metrics_per_week(df)

    # Then: It should return the expected columns
    assert list(result.columns) == [
        "startdate",
        "enddate",
        "cycletime_p50",
        "cycletime_p85",
        "throughput",
    ]


def test_check_statuses_defined_undefined_statuses():
    """
    This test ensures that the check_statuses_defined function
    identifies and raises an exception for any status that is
    not defined in the configuration.
    Useful for: Ensuring that all statuses in the dataframe are
    expected and accounted for in the configuration.
    """
    # Given: A dataframe with some statuses and a configuration that does not account for all of them
    data = {"to_status": ["Backlog", "In Progress", "Done", "UndefinedStatus"]}
    dataframe = pd.DataFrame(data)
    cfg = {
        "todo_names": ["Backlog", "To Do"],
        "wip_names": ["In Progress", "Review & QA"],
        "done_names": ["Done"],
    }

    # When: We check the dataframe against the configuration
    # Then: An exception should be raised for the undefined status
    with pytest.raises(ValueError, match=r"(?i)UndefinedStatus"):
        check_statuses_defined(dataframe, cfg)


def test_check_statuses_defined_all_defined():
    """
    This test ensures that the check_statuses_defined function
    does not raise any exceptions when all statuses in the dataframe
    are defined in the configuration.
    Useful for: Validating that our function does not falsely flag
    valid configurations.
    """
    # Given: A dataframe with some statuses and a configuration that accounts for all of them
    data = {"to_status": ["Backlog", "In Progress", "Done"]}
    dataframe = pd.DataFrame(data)
    cfg = {
        "todo_names": ["Backlog", "To Do"],
        "wip_names": ["In Progress", "Review & QA"],
        "done_names": ["Done"],
    }

    # When: We check the dataframe against the configuration
    # Then: No exception should be raised
    check_statuses_defined(dataframe, cfg)
