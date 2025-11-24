"""
Description: Data Quality checks for facility_name_min_time_spent_per_visit_date dataset.
Requirement(s): TICKET-1234
Author(s): Name Surname
"""

import pytest


@pytest.fixture(scope='module')
def source_data(db_connection):
    """
    Fixture to load source data from PostgreSQL database.
    
    This query retrieves the expected data for facility_name_min_time_spent_per_visit_date
    from the normalized tables in the database.
    
    Args:
        db_connection: Database connection fixture from conftest.py
    
    Returns:
        pd.DataFrame: Source data from PostgreSQL
    """
    source_query = """
    SELECT
        f.facility_name,
        v.visit_timestamp::date AS visit_date,
        MIN(v.duration_minutes) AS min_time_spent
    FROM
        visits v
    JOIN facilities f 
        ON f.id = v.facility_id
    GROUP BY
        f.facility_name,
        visit_date
    ORDER BY
        f.facility_name,
        visit_date;
    """
    source_data = db_connection.get_data_sql(source_query)
    return source_data


@pytest.fixture(scope='module')
def target_data(parquet_reader):
    """
    Fixture to load target data from Parquet files.
    
    This reads all parquet files from the facility_name_min_time_spent_per_visit_date
    directory, including partitioned subdirectories.
    
    Args:
        parquet_reader: Parquet reader fixture from conftest.py
    
    Returns:
        pd.DataFrame: Target data from Parquet files
    """
    target_path = '/parquet_data/facility_name_min_time_spent_per_visit_date'
    target_data = parquet_reader.process(target_path, include_subfolders=True)
    
    return target_data


# ============================================================================
# SMOKE TESTS
# ============================================================================

@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_target_dataset_is_not_empty(target_data, data_quality_library):
    """
    Smoke test to verify that the target parquet dataset is not empty.
    
    This is a basic sanity check to ensure data was loaded successfully.
    """
    data_quality_library.check_dataset_is_not_empty(target_data)


@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_source_dataset_is_not_empty(source_data, data_quality_library):
    """
    Smoke test to verify that the source database dataset is not empty.
    
    This ensures the source query returns data for comparison.
    """
    data_quality_library.check_dataset_is_not_empty(source_data)


# ============================================================================
# DATA COMPLETENESS TESTS
# ============================================================================

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_row_count(source_data, target_data, data_quality_library):
    """
    Test to verify that source and target have the same number of rows.
    
    This is a quick completeness check before detailed comparison.
    """
    data_quality_library.check_count(source_data, target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_data_completeness(source_data, target_data, data_quality_library):
    """
    Test to verify that all data from source exists in target with exact values.
    
    This performs a comprehensive row-by-row comparison between source and target.
    Known Issue: The source query contains a UNION ALL that creates duplicates
                for 'Clinic' facility types, which should fail this test.
    """
    data_quality_library.check_data_completeness(source_data, target_data)


# ============================================================================
# DATA QUALITY TESTS
# ============================================================================

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_no_duplicates_in_target(target_data, data_quality_library):
    """
    Test to verify that there are no duplicate rows in the target dataset.
    
    Each combination of facility_name and visit_date should be unique.
    Known Issue: Due to UNION ALL in the transformation query, duplicates
                are expected for 'Clinic' facility types.
    """
    data_quality_library.check_duplicates(
        target_data,
        column_names=['facility_name', 'visit_date']
    )


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_not_null_values_in_target(target_data, data_quality_library):
    """
    Test to verify that critical columns do not contain null values.
    
    All columns in this dataset should be non-nullable.
    """
    data_quality_library.check_not_null_values(
        target_data,
        column_names=['facility_name', 'visit_date', 'min_time_spent']
    )
