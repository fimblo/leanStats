import pytest
import pandas as pd
import numpy as np
from io import StringIO
import sys
import os

from leanStats import (
    extract_ticket_timestamps,
    calculate_cycletime,
    compute_metrics,
    weekly_metrics,
)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def sample_data_time():
    data = """Key,Status Transition.date,Status Transition.to
TICKET-1,15/09/2023 00:01:00,IN PROGRESS
TICKET-1,17/09/2023 00:02:00,DONE
TICKET-2,16/09/2023 00:01:00,IN PROGRESS
TICKET-2,20/09/2023 00:02:00,DONE
"""
    return StringIO(data)


@pytest.fixture
def sample_data_date():
    data = """Key,Status Transition.date,Status Transition.to
TICKET-1,15/09/2023,IN PROGRESS
TICKET-1,17/09/2023,DONE
TICKET-2,16/09/2023,IN PROGRESS
TICKET-2,20/09/2023,DONE
"""
    return StringIO(data)


def test_extract_ticket_datestamps(sample_data_date):
    data = pd.read_csv(
        sample_data_date, parse_dates=["Status Transition.date"], dayfirst=True
    )
    df = extract_ticket_timestamps(data)

    assert len(df) == 2
    assert df.iloc[0]["Key"] == "TICKET-1"
    assert df.iloc[0]["timestamp_start"].strftime("%Y-%m-%d") == "2023-09-15"
    assert df.iloc[0]["timestamp_end"].strftime("%Y-%m-%d") == "2023-09-17"
    assert df.iloc[1]["Key"] == "TICKET-2"
    assert df.iloc[1]["timestamp_start"].strftime("%Y-%m-%d") == "2023-09-16"
    assert df.iloc[1]["timestamp_end"].strftime("%Y-%m-%d") == "2023-09-20"


def test_extract_ticket_timestamps(sample_data_time):
    data = pd.read_csv(
        sample_data_time, parse_dates=["Status Transition.date"], dayfirst=True
    )
    df = extract_ticket_timestamps(data)

    assert len(df) == 2
    assert df.iloc[0]["Key"] == "TICKET-1"
    assert (
        df.iloc[0]["timestamp_start"].strftime("%Y-%m-%d %H:%M:%S")
        == "2023-09-15 00:01:00"
    )
    assert (
        df.iloc[0]["timestamp_end"].strftime("%Y-%m-%d %H:%M:%S")
        == "2023-09-17 00:02:00"
    )
    assert df.iloc[1]["Key"] == "TICKET-2"
    assert (
        df.iloc[1]["timestamp_start"].strftime("%Y-%m-%d %H:%M:%S")
        == "2023-09-16 00:01:00"
    )
    assert (
        df.iloc[1]["timestamp_end"].strftime("%Y-%m-%d %H:%M:%S")
        == "2023-09-20 00:02:00"
    )


def test_calculate_cycletime(sample_data_time):
    data = pd.read_csv(
        sample_data_time, parse_dates=["Status Transition.date"], dayfirst=True
    )
    df = extract_ticket_timestamps(data)
    df = calculate_cycletime(df)

    # we always round cycle time up.
    assert len(df) == 2
    assert (
        df.iloc[0]["cycletime"] == 3
    )  # 3 days from '2023-09-15' to '2023-09-17 + 1 minute'
    assert (
        df.iloc[1]["cycletime"] == 5
    )  # 5 days from '2023-09-16' to '2023-09-20 + 1 minute'


def test_compute_metrics_basic_functionality():
    # Given: Create a mock dataframe
    data = {
        "Key": ["A", "B", "C", "D"],
        "timestamp_end": [
            pd.Timestamp("2023-09-15 10:00:00"),
            pd.Timestamp("2023-09-17 10:00:00"),
            pd.Timestamp("2023-09-19 10:00:00"),
            pd.Timestamp("2023-09-20 10:00:00"),
        ],
        "cycletime": [5, 3, 4, 2],
    }
    df = pd.DataFrame(data)

    # When: Run the compute_metrics function
    result_df = compute_metrics(df)

    # Then: Check the basic functionality

    # Check columns are present
    assert "median_cycletime" in result_df.columns
    assert "p85_cycletime" in result_df.columns
    assert "throughput" in result_df.columns

    # Check correct calculations for each ticket
    assert result_df.loc[result_df["Key"] == "A", "median_cycletime"].values[0] == 5
    assert result_df.loc[result_df["Key"] == "A", "p85_cycletime"].values[0] == 5
    assert result_df.loc[result_df["Key"] == "A", "throughput"].values[0] == 1

    assert result_df.loc[result_df["Key"] == "B", "median_cycletime"].values[0] == 4
    assert result_df.loc[result_df["Key"] == "B", "p85_cycletime"].values[0] == 5
    assert result_df.loc[result_df["Key"] == "B", "throughput"].values[0] == 2

    assert result_df.loc[result_df["Key"] == "C", "median_cycletime"].values[0] == 4
    assert result_df.loc[result_df["Key"] == "C", "p85_cycletime"].values[0] == 5
    assert result_df.loc[result_df["Key"] == "C", "throughput"].values[0] == 3

    assert result_df.loc[result_df["Key"] == "D", "median_cycletime"].values[0] == 4
    assert result_df.loc[result_df["Key"] == "D", "p85_cycletime"].values[0] == 5
    assert result_df.loc[result_df["Key"] == "D", "throughput"].values[0] == 4


def test_basic_input():
    # Given: A basic input DataFrame
    data = {
        "timestamp_end": ["2023-01-01", "2023-01-03", "2023-01-05"],
        "cycletime": [5, 7, 6],
        "Key": [1, 2, 3],
    }
    df = pd.DataFrame(data)
    df["timestamp_end"] = pd.to_datetime(df["timestamp_end"])

    # When: Calling the weekly_metrics function
    result = weekly_metrics(df)

    # Then: It should handle and return the expected output
    expected = [(pd.Timestamp("2023-01-02"), pd.Timestamp("2023-01-08"), 6.0, 7.0, 3)]
    assert list(result.itertuples(index=False, name=None)) == expected


def test_fill_missing_week():
    # Given: Input data with a gap week
    data = {
        "timestamp_end": ["2023-01-01", "2023-01-15"],
        "cycletime": [5, 7],
        "Key": [1, 2],
    }
    df = pd.DataFrame(data)
    df["timestamp_end"] = pd.to_datetime(df["timestamp_end"])

    # When: Calling the weekly_metrics function
    result = weekly_metrics(df)

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


def test_expected_columns():
    # Given: Any DataFrame
    data = {
        "timestamp_end": ["2023-01-01", "2023-01-03"],
        "cycletime": [5, 6],
        "Key": [1, 2],
    }
    df = pd.DataFrame(data)
    df["timestamp_end"] = pd.to_datetime(df["timestamp_end"])

    # When: Calling the weekly_metrics function
    result = weekly_metrics(df)

    # Then: It should return the expected columns
    assert list(result.columns) == [
        "startdate",
        "enddate",
        "cycletime_p50",
        "cycletime_p85",
        "throughput",
    ]
