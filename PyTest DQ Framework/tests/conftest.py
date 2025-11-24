import pytest
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.data_quality.data_quality_validation_library import DataQualityLibrary
from src.connectors.file_system.parquet_reader import ParquetReader

def pytest_addoption(parser):
    """
    Add custom command-line options for database connection configuration.
    
    These options allow users to specify database connection details when running tests.
    """
    parser.addoption("--db_host", action="store", default="localhost", help="Database host")
    parser.addoption("--db_name", action="store", default="mydatabase", help="Database name")
    parser.addoption("--db_port", action="store", default="5434", help="Database port")
    parser.addoption("--db_user", action="store", help="Database user (required)")
    parser.addoption("--db_password", action="store", help="Database password (required)")


def pytest_configure(config):
    """
    Validates that all required command-line options are provided.
    
    Args:
        config: Pytest configuration object.
    
    Raises:
        pytest.UsageError: If any required option is missing.
    """
    required_options = [
        "--db_user", "--db_password"
    ]
    for option in required_options:
        if not config.getoption(option):
            pytest.fail(f"Missing required option: {option}")
    
    # Register custom markers
    config.addinivalue_line(
        "markers", "parquet_data: Mark tests for parquet data validation"
    )
    config.addinivalue_line(
        "markers", "smoke: Mark tests as smoke tests"
    )
    config.addinivalue_line(
        "markers", "facility_name_min_time_spent_per_visit_date: Tests for facility_name_min_time_spent_per_visit_date dataset"
    )
    config.addinivalue_line(
        "markers", "facility_type_avg_time_spent_per_visit_date: Tests for facility_type_avg_time_spent_per_visit_date dataset"
    )
    config.addinivalue_line(
        "markers", "patient_sum_treatment_cost_per_facility_type: Tests for patient_sum_treatment_cost_per_facility_type dataset"
    )


@pytest.fixture(scope='session')
def db_connection(request):
    """
    Fixture to establish a database connection for the test session.
    
    This fixture creates a PostgreSQL connection using the command-line options
    provided by the user. The connection is shared across all tests in the session.
    
    Args:
        request: Pytest request object to access command-line options.
    
    Yields:
        PostgresConnectorContextManager: Database connection object.
    
    Raises:
        pytest.fail: If the database connection cannot be established.
    """
    db_host = request.config.getoption("--db_host")
    db_name = request.config.getoption("--db_name")
    db_port = request.config.getoption("--db_port")
    db_user = request.config.getoption("--db_user")
    db_password = request.config.getoption("--db_password")

    try:
        with PostgresConnectorContextManager(
            db_user=db_user,
            db_password=db_password,
            db_host=db_host,
            db_name=db_name,
            db_port=int(db_port)
        ) as db_connector:
            yield db_connector
    except Exception as e:
        pytest.fail(f"Failed to initialize PostgresConnectorContextManager: {e}")


@pytest.fixture(scope='session')
def parquet_reader(request):
    """
    Fixture to create a ParquetReader instance for the test session.
    
    This fixture provides a ParquetReader object that can be used to read
    and process Parquet files stored in the file system.
    
    Args:
        request: Pytest request object.
    
    Yields:
        ParquetReader: Parquet reader object.
    
    Raises:
        pytest.fail: If the ParquetReader cannot be initialized.
    """
    try:
        reader = ParquetReader()
        yield reader
    except Exception as e:
        pytest.fail(f"Failed to initialize ParquetReader: {e}")
    finally:
        del reader


@pytest.fixture(scope='session')
def data_quality_library():
    """
    Fixture to create a DataQualityLibrary instance for the test session.
    
    This fixture provides a DataQualityLibrary object containing methods
    for performing data quality checks on DataFrames.
    
    Yields:
        DataQualityLibrary: Data quality library object.
    
    Raises:
        pytest.fail: If the DataQualityLibrary cannot be initialized.
    """
    try:
        data_quality_library = DataQualityLibrary()
        yield data_quality_library
    except Exception as e:
        pytest.fail(f"Failed to initialize DataQualityLibrary: {e}")
    finally:
        del data_quality_library