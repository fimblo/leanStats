#!/usr/bin/env python3

import pandas as pd
import numpy as np
from pprint import pprint
import sys

def read_and_output_csv(file_path):
    """Read a CSV file and print it to stdout."""
    data = pd.read_csv(file_path)


    for index, row in data.iterrows():
        output_row = '->'.join(['{}'.format(cell) for cell in row])
        print(output_row)


def extract_ticket_timestamps(file_path):

    data = pd.read_csv(file_path,
                       parse_dates=['Status Transition.date'],
                       dayfirst=True) # dd/mm/yy madness
    
    # filter when tickets moved to IN PROGRESS
    in_progress = (data[data['Status Transition.to'].str.upper() == 'IN PROGRESS']
                   .groupby('Key')
                   .agg({'Status Transition.date': 'min'})
                   .rename(columns={'Status Transition.date': 'timestamp_start'}))
    
    # filter when tickets moved to DONE
    done = (data[data['Status Transition.to'].str.upper() == 'DONE']
            .groupby('Key')
            .agg({'Status Transition.date': 'max'})
            .rename(columns={'Status Transition.date': 'timestamp_end'}))
    
    # left join on key.
    result = in_progress.join(done, on='Key').reset_index()

    # Calculate cycle time, rounding up to the nearest day
    time_difference     = result['timestamp_end'] - result['timestamp_start']
    total_seconds       = time_difference.dt.total_seconds()
    cycletime_in_days   = np.ceil(total_seconds / (24 * 3600))
    result['cycletime'] = cycletime_in_days.astype(int)

    return result


if __name__ == "__main__":
    # Check if a file path is provided
    if len(sys.argv) < 2:
        print("Please provide a file path as an argument.")
        sys.exit(1)
    file_path = sys.argv[1]

    dataframe = extract_ticket_timestamps(file_path)

    
    tickets_data = dataframe[['Key',
                              'timestamp_start',
                              'timestamp_end',
                              'cycletime']].values.tolist()

    pprint(tickets_data)

