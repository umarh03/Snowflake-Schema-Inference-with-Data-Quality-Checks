import unittest
from source import get_source_data
from destination import get_destination_data

from etl_utils import (
    compare_row_counts,
    compare_schemas,
    test_extra_or_missing_columns,
    test_no_null_values_in_critical_columns,
    test_column_data_types,
    test_no_duplicate_rows_in_destination,
     test_row_order_consistency,
    test_numeric_column_consistency
)

class ETL_Test(unittest.TestCase):

    def setUp(self):
        # Fetch the data
        self.source_data = get_source_data()
        self.destination_data = get_destination_data()

    def test_row_count_match(self):
       compare_row_counts(self.source_data, self.destination_data)

    def test_schema_match(self):
       compare_schemas(self.source_data, self.destination_data)


    def test_extra_or_missing_columns(self):
        test_extra_or_missing_columns(self.source_data, self.destination_data)

    def test_no_null_values_in_critical_columns(self):
        test_no_null_values_in_critical_columns(self.source_data, self.destination_data)

    def test_column_data_types(self):
        test_column_data_types(self.source_data, self.destination_data)

    def test_no_duplicate_rows_in_destination(self):
        test_no_duplicate_rows_in_destination(self.destination_data)

    def test_row_order_consistency(self):
        test_row_order_consistency(self.source_data, self.destination_data)

    def test_numeric_column_consistency(self):
        test_numeric_column_consistency(self.source_data, self.destination_data)

if __name__ == "__main__":
    unittest.main()