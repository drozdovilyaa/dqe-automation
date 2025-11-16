import pytest
import re


def test_file_not_empty(csv_data):
    """
    Test 1: Validate that file is not empty.
    Mark: - (unmarked, will be auto-marked by hook)
    """
    assert len(csv_data) > 0, "CSV file should not be empty"
    assert not csv_data.empty, "CSV DataFrame should contain data"


@pytest.mark.validate_csv
def test_validate_schema(csv_data):
    """
    Test 2: Validate the schema of the file (id, name, age, email).
    Mark: validate_csv
    """
    expected_schema = ['id', 'name', 'age', 'email', 'is_active']
    actual_schema = list(csv_data.columns)
    
    assert actual_schema == expected_schema, \
        f"Schema mismatch: Expected {expected_schema}, but got {actual_schema}"


@pytest.mark.validate_csv
@pytest.mark.skip(reason="Skipping age validation as per assignment requirements")
def test_age_column_valid(csv_data):
    """
    Test 3: Validate that the age column contains valid values (0-100).
    Mark: validate_csv, skip
    """
    invalid_ages = csv_data[(csv_data['age'] < 0) | (csv_data['age'] > 100)]
    
    assert invalid_ages.empty, \
        f"Found {len(invalid_ages)} rows with invalid age values (not in range 0-100): {invalid_ages['age'].tolist()}"


@pytest.mark.validate_csv
def test_email_column_valid(csv_data):
    """
    Test 4: Validate that the email column contains valid email addresses (format).
    Mark: validate_csv
    """
    # Email regex pattern
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Check each email
    invalid_emails = []
    for idx, email in csv_data['email'].items():
        if not re.match(email_pattern, str(email)):
            invalid_emails.append((idx, email))
    
    assert len(invalid_emails) == 0, \
        f"Found {len(invalid_emails)} invalid email addresses: {invalid_emails}"


@pytest.mark.validate_csv
@pytest.mark.xfail(reason="Expecting duplicate rows in the dataset")
def test_duplicates(csv_data):
    """
    Test 5: Validate there are no duplicate rows.
    Mark: validate_csv, xfail
    """
    duplicate_count = csv_data.duplicated().sum()
    
    assert duplicate_count == 0, \
        f"Found {duplicate_count} duplicate rows in the CSV file"


@pytest.mark.parametrize("id_value,expected_is_active", [
    (1, False),
    (2, True)
])
def test_active_players(csv_data, id_value, expected_is_active):
    """
    Test 6: Validate that:
    - is_active = False for id = 1.
    - is_active = True for id = 2.
    Mark: parametrize("id, is_active", [...])
    """
    # Filter by id
    row = csv_data[csv_data['id'] == id_value]
    
    assert not row.empty, \
        f"No row found with id = {id_value}"
    
    actual_is_active = row.iloc[0]['is_active']
    
    assert actual_is_active == expected_is_active, \
        f"For id={id_value}, expected is_active={expected_is_active}, but got {actual_is_active}"


def test_active_player(csv_data):
    """
    Test 7: Same as previous one for id = 2, but without parametrize mark.
    Mark: - (unmarked, will be auto-marked by hook)
    """
    id_value = 2
    expected_is_active = True
    
    # Filter by id
    row = csv_data[csv_data['id'] == id_value]
    
    assert not row.empty, \
        f"No row found with id = {id_value}"
    
    actual_is_active = row.iloc[0]['is_active']
    
    assert actual_is_active == expected_is_active, \
        f"For id={id_value}, expected is_active={expected_is_active}, but got {actual_is_active}"
