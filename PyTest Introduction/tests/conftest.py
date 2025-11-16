import pytest
import pandas as pd
import os


# Pytest hook to register custom ini option
def pytest_addoption(parser):
    """
    Register custom ini configuration option for CSV file path.
    """
    parser.addini(
        "csv_file_path",
        help="Path to the CSV file to be used in tests",
        default="../src/data/data.csv"
    )


# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(config, items):
    """
    Hook to dynamically mark tests that do not have explicit marks.
    Tests without marks will be assigned to the custom mark 'unmarked'.
    """
    for item in items:
        # Get all marks for the test
        marks = [mark.name for mark in item.iter_markers()]
        
        # If the test has no marks (excluding 'unmarked'), add the 'unmarked' mark
        if not marks or (len(marks) == 1 and 'unmarked' in marks):
            item.add_marker(pytest.mark.unmarked)


# Fixture to read the CSV file
@pytest.fixture(scope="session")
def csv_data(request):
    """
    Fixture to read the CSV file and return its content.
    Path is read from pytest.ini configuration (csv_file_path).
    
    Returns:
        pandas.DataFrame: The CSV content as a DataFrame.
    """
    # Read path from pytest.ini
    path_to_file = request.config.getini("csv_file_path")
    
    if not path_to_file:
        pytest.fail("csv_file_path must be specified in pytest.ini")
    
    # Normalize the path
    path_to_file = os.path.normpath(path_to_file)
    
    # Check if file exists
    if not os.path.exists(path_to_file):
        pytest.fail(f"CSV file not found: {path_to_file}")
    
    # Read the CSV file
    try:
        df = pd.read_csv(path_to_file)
        return df
    except Exception as e:
        pytest.fail(f"Failed to read CSV file: {e}")


# Fixture to validate the schema of the file
@pytest.fixture(scope="session")
def validate_schema(actual_schema, expected_schema):
    """
    Fixture to validate the schema of the file.
    
    Args:
        actual_schema: The actual schema (list of column names).
        expected_schema: The expected schema (list of column names).
    
    Returns:
        bool: True if schemas match, False otherwise.
    """
    return list(actual_schema) == list(expected_schema)
