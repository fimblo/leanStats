import pytest
import pandas as pd
from io import StringIO
import sys
import os

from leanStats import extract_ticket_timestamps, calculate_cycletime, compute_metrics
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
    df = extract_ticket_timestamps(sample_data_date)

    assert len(df) == 2
    assert df.iloc[0]['Key'] == 'TICKET-1'
    assert df.iloc[0]['timestamp_start'].strftime('%Y-%m-%d') == '2023-09-15'
    assert df.iloc[0]['timestamp_end'].strftime('%Y-%m-%d') == '2023-09-17'
    assert df.iloc[1]['Key'] == 'TICKET-2'
    assert df.iloc[1]['timestamp_start'].strftime('%Y-%m-%d') == '2023-09-16'
    assert df.iloc[1]['timestamp_end'].strftime('%Y-%m-%d') == '2023-09-20'



def test_extract_ticket_timestamps(sample_data_time):
    df = extract_ticket_timestamps(sample_data_time)

    assert len(df) == 2
    assert df.iloc[0]['Key'] == 'TICKET-1'
    assert df.iloc[0]['timestamp_start'].strftime('%Y-%m-%d %H:%M:%S') == '2023-09-15 00:01:00'
    assert df.iloc[0]['timestamp_end'].strftime('%Y-%m-%d %H:%M:%S') == '2023-09-17 00:02:00'
    assert df.iloc[1]['Key'] == 'TICKET-2'
    assert df.iloc[1]['timestamp_start'].strftime('%Y-%m-%d %H:%M:%S') == '2023-09-16 00:01:00'
    assert df.iloc[1]['timestamp_end'].strftime('%Y-%m-%d %H:%M:%S') == '2023-09-20 00:02:00'



def test_calculate_cycletime(sample_data_time):
    df = extract_ticket_timestamps(sample_data_time)
    df = calculate_cycletime(df)

    # we always round cycle time up. 
    assert len(df) == 2
    assert df.iloc[0]['cycletime'] == 3  # 3 days from '2023-09-15' to '2023-09-17 + 1 minute'
    assert df.iloc[1]['cycletime'] == 5  # 5 days from '2023-09-16' to '2023-09-20 + 1 minute'



def test_compute_metrics_basic_functionality():
    # Given: Create a mock dataframe
    data = {
        'Key': ['A', 'B', 'C', 'D'],
        'timestamp_end': [pd.Timestamp('2023-09-15 10:00:00'),
                          pd.Timestamp('2023-09-17 10:00:00'),
                          pd.Timestamp('2023-09-19 10:00:00'),
                          pd.Timestamp('2023-09-20 10:00:00')],
        'cycletime': [5, 3, 4, 2]
    }
    df = pd.DataFrame(data)
    
    # When: Run the compute_metrics function
    result_df = compute_metrics(df)
    
    # Then: Check the basic functionality

    # Check columns are present
    assert 'median_cycletime' in result_df.columns
    assert 'p85_cycletime' in result_df.columns
    assert 'throughput' in result_df.columns

    # Check correct calculations for each ticket
    assert result_df.loc[result_df['Key'] == 'A', 'median_cycletime'].values[0] == 5
    assert result_df.loc[result_df['Key'] == 'A', 'p85_cycletime'].values[0] == 5
    assert result_df.loc[result_df['Key'] == 'A', 'throughput'].values[0] == 1

    assert result_df.loc[result_df['Key'] == 'B', 'median_cycletime'].values[0] == 4
    assert result_df.loc[result_df['Key'] == 'B', 'p85_cycletime'].values[0] == 5
    assert result_df.loc[result_df['Key'] == 'B', 'throughput'].values[0] == 2

    assert result_df.loc[result_df['Key'] == 'C', 'median_cycletime'].values[0] == 4
    assert result_df.loc[result_df['Key'] == 'C', 'p85_cycletime'].values[0] == 5
    assert result_df.loc[result_df['Key'] == 'C', 'throughput'].values[0] == 3

    assert result_df.loc[result_df['Key'] == 'D', 'median_cycletime'].values[0] == 4
    assert result_df.loc[result_df['Key'] == 'D', 'p85_cycletime'].values[0] == 5
    assert result_df.loc[result_df['Key'] == 'D', 'throughput'].values[0] == 4

