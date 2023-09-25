#!/usr/bin/env python3

import pandas as pd
import numpy as np
#from pprint import pprint
import sys

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
    return in_progress.join(done, on='Key').reset_index()



def calculate_cycletime(dataframe):
    df_copy = dataframe.copy()
    
    time_difference      = df_copy['timestamp_end'] - df_copy['timestamp_start']
    total_seconds        = time_difference.dt.total_seconds()
    cycletime_in_days    = np.ceil(total_seconds / (24 * 3600))
    df_copy['cycletime'] = cycletime_in_days.astype(int)

    return df_copy



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide a file path as an argument.")
        sys.exit(1)
    file_path = sys.argv[1]

    # read in data and calculate cycletime
    dataframe = extract_ticket_timestamps(file_path)
    dataframe = calculate_cycletime(dataframe)

    # select which fields I want
    dataframe = dataframe[['Key', 'timestamp_start', 'timestamp_end', 'cycletime']]


    # Print to stdout
    tickets_data = dataframe.sort_values(by='timestamp_end').values.tolist()
    headers = ['Key', 'timestamp_start', 'timestamp_end', 'cycletime']
    print("|".join(headers))
    for row in tickets_data:
        print("|".join(map(str, row)))


