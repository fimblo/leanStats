import os
from io import StringIO
import sys
import pytest
import pandas as pd
# This allows us to import the main script functions
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.leanStats import read_and_output_csv

def test_read_and_output_csv(capfd):
    """Test reading a CSV and outputting to stdout."""
    
    # Using a mock CSV for testing
    mock_csv = "tests/simple-test-data.csv"
    
    read_and_output_csv(mock_csv)
    
    # Capture the stdout output
    captured = capfd.readouterr().out.strip()

    # Load the original CSV to a DataFrame
    original_data = pd.read_csv(mock_csv)
    
    # Convert the captured stdout back to a DataFrame
    output_data = pd.read_csv(StringIO(captured))

    # Check if the dataframes are the same
    pd.testing.assert_frame_equal(original_data, output_data)

if __name__ == "__main__":
    pytest.main()
