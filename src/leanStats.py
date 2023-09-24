#!/usr/bin/env python3

import pandas as pd
import sys

def read_and_output_csv(file_path):
    """Read a CSV file and print it to stdout."""
    data = pd.read_csv(file_path)
    # Convert the DataFrame to CSV format and print
    print(data.to_csv(index=False))

if __name__ == "__main__":
    # Check if a file path is provided
    if len(sys.argv) < 2:
        print("Please provide a file path as an argument.")
        sys.exit(1)
    
    file_path = sys.argv[1]
    read_and_output_csv(file_path)
