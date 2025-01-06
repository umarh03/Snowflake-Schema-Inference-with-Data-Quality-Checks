import pandas as pd


def compare_row_counts(source_data, destination_data):
    assert len(source_data) == len(destination_data), 'f"Row count mismatch'


def compare_schemas(source_data, destination_data):
    assert list(source_data.columns) == list(destination_data.columns), 'f"Schema mismatch'


def test_extra_or_missing_columns(source_data, destination_data):
    source_columns = set(source_data.columns)
    destination_columns = set(destination_data.columns)

    extra_columns = destination_columns - source_columns
    missing_columns = source_columns - destination_columns

    assert not extra_columns, f"Extra columns in destination: {extra_columns}"
    assert not missing_columns, f"Missing columns in destination: {missing_columns}"


def test_no_null_values_in_critical_columns(source_data, destination_data):
    critical_columns = ['Make', 'Model']  # Adjust based on your data
    for column in critical_columns:
        assert source_data[column].isnull().sum() == 0, f"Null values found in source column: {column}"
        assert destination_data[column].isnull().sum() == 0, f"Null values found in destination column: {column}"


def test_column_data_types(source_data, destination_data):
    for column in source_data.columns:
        source_dtype = source_data[column].dtype
        destination_dtype = destination_data[column].dtype
        assert source_dtype == destination_dtype, f"Data type mismatch for column {column}: {source_dtype} != {destination_dtype}"


def test_no_duplicate_rows_in_destination(destination_data):
    assert destination_data.duplicated().sum() == 0, "Duplicate rows found in destination data"

def test_row_order_consistency(source_data, destination_data):
    assert source_data.reset_index(drop=True).equals(
        destination_data.reset_index(drop=True)), "Row order mismatch between source and destination"

def test_numeric_column_consistency(source_data, destination_data, numeric_column='MSRP'):
    assert source_data[numeric_column].equals(
        destination_data[numeric_column]), f"Numeric column {numeric_column} mismatch between source and destination"
