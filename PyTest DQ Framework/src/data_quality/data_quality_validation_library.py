import pandas as pd
from typing import List, Optional


class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    This class is intended to be used in a PyTest-based testing framework to validate
    the quality of data in DataFrames. Each method performs a specific data quality
    check and uses assertions to ensure that the data meets the expected conditions.
    """

    @staticmethod
    def check_duplicates(df: pd.DataFrame, column_names: Optional[List[str]] = None, max_display: int = 10) -> None:
        """
        Check for duplicate rows in the DataFrame.
        
        Args:
            df (pd.DataFrame): The DataFrame to check for duplicates.
            column_names (Optional[List[str]]): List of column names to check for duplicates.
                                               If None, checks all columns.
            max_display (int): Maximum number of duplicate rows to display in error message.
        
        Raises:
            AssertionError: If duplicate rows are found.
        """
        if column_names:
            duplicates = df[df.duplicated(subset=column_names, keep=False)]
        else:
            duplicates = df[df.duplicated(keep=False)]
        
        if len(duplicates) > 0:
            sample_duplicates = duplicates.head(max_display)
            more_rows = len(duplicates) - max_display if len(duplicates) > max_display else 0
            
            error_msg = (
                f"Found {len(duplicates)} duplicate rows in the dataset.\n\n"
                f"First {min(len(duplicates), max_display)} duplicate rows:\n"
                f"{sample_duplicates.to_string()}\n"
            )
            
            if more_rows > 0:
                error_msg += f"\n... and {more_rows} more duplicate rows not shown."
            
            assert False, error_msg

    @staticmethod
    def check_count(source_df: pd.DataFrame, target_df: pd.DataFrame) -> None:
        """
        Check that the row count of two DataFrames is equal.
        
        Args:
            source_df (pd.DataFrame): The source DataFrame.
            target_df (pd.DataFrame): The target DataFrame to compare against.
        
        Raises:
            AssertionError: If the row counts do not match.
        """
        source_count = len(source_df)
        target_count = len(target_df)
        
        assert source_count == target_count, (
            f"Row count mismatch: Source has {source_count} rows, "
            f"but Target has {target_count} rows. "
            f"Difference: {abs(source_count - target_count)} rows."
        )

    @staticmethod
    def check_data_completeness(source_df: pd.DataFrame, target_df: pd.DataFrame, max_display: int = 10) -> None:
        """
        Check that all rows from the source DataFrame are present in the target DataFrame.
        
        This method compares the source and target DataFrames to ensure that all data
        from the source is present in the target. It performs a full comparison of all
        columns and rows.
        
        Args:
            source_df (pd.DataFrame): The source DataFrame (expected data).
            target_df (pd.DataFrame): The target DataFrame (actual data).
            max_display (int): Maximum number of rows to display in error message.
        
        Raises:
            AssertionError: If there are missing rows or mismatched data.
        """
        # Reset indices to ensure proper comparison
        source_df_reset = source_df.reset_index(drop=True)
        target_df_reset = target_df.reset_index(drop=True)
        
        # Sort both dataframes by all columns to ensure consistent ordering
        source_df_sorted = source_df_reset.sort_values(by=list(source_df_reset.columns)).reset_index(drop=True)
        target_df_sorted = target_df_reset.sort_values(by=list(target_df_reset.columns)).reset_index(drop=True)
        
        # Check if dataframes are equal
        try:
            pd.testing.assert_frame_equal(
                source_df_sorted,
                target_df_sorted,
                check_dtype=False,  # Allow type differences
                check_exact=False,  # Allow small floating-point differences
                rtol=1e-5  # Relative tolerance for floating-point comparison
            )
        except (AssertionError, ValueError) as e:
            # Find missing rows
            error_msg = "Data completeness check failed:\n\n"
            
            # Add row count comparison
            error_msg += f"Row count - Source: {len(source_df_sorted)}, Target: {len(target_df_sorted)}\n"
            error_msg += f"Difference: {abs(len(source_df_sorted) - len(target_df_sorted))} rows\n\n"
            
            # Try to find specific differences
            try:
                merged = source_df_sorted.merge(
                    target_df_sorted,
                    how='outer',
                    indicator=True
                )
                
                missing_in_target = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
                extra_in_target = merged[merged['_merge'] == 'right_only'].drop('_merge', axis=1)
                
                if len(missing_in_target) > 0:
                    sample_missing = missing_in_target.head(max_display)
                    more_missing = len(missing_in_target) - max_display if len(missing_in_target) > max_display else 0
                    
                    error_msg += f"Rows missing in target ({len(missing_in_target)} total):\n"
                    error_msg += f"{sample_missing.to_string()}\n"
                    if more_missing > 0:
                        error_msg += f"... and {more_missing} more rows not shown.\n"
                    error_msg += "\n"
                
                if len(extra_in_target) > 0:
                    sample_extra = extra_in_target.head(max_display)
                    more_extra = len(extra_in_target) - max_display if len(extra_in_target) > max_display else 0
                    
                    error_msg += f"Extra rows in target not in source ({len(extra_in_target)} total):\n"
                    error_msg += f"{sample_extra.to_string()}\n"
                    if more_extra > 0:
                        error_msg += f"... and {more_extra} more rows not shown.\n"
            
            except Exception as merge_error:
                # If merge fails (e.g., due to data type issues), provide basic info
                error_msg += f"\nUnable to perform detailed comparison: {str(merge_error)}\n"
                error_msg += "\nPossible causes:\n"
                error_msg += "- Data type mismatches between source and target\n"
                error_msg += "- Column name differences\n"
                error_msg += "- Date/datetime format inconsistencies\n"
            
            raise AssertionError(error_msg)

    @staticmethod
    def check_dataset_is_not_empty(df: pd.DataFrame) -> None:
        """
        Check that the DataFrame is not empty.
        
        Args:
            df (pd.DataFrame): The DataFrame to check.
        
        Raises:
            AssertionError: If the DataFrame is empty.
        """
        assert not df.empty, "Dataset is empty. Expected at least one row."
        assert len(df.columns) > 0, "Dataset has no columns."

    @staticmethod
    def check_not_null_values(df: pd.DataFrame, column_names: List[str]) -> None:
        """
        Check that specified columns do not contain null values.
        
        Args:
            df (pd.DataFrame): The DataFrame to check.
            column_names (List[str]): List of column names to check for null values.
        
        Raises:
            AssertionError: If null values are found in any of the specified columns.
        """
        null_columns = []
        
        for column in column_names:
            if column not in df.columns:
                raise ValueError(f"Column '{column}' does not exist in the DataFrame.")
            
            null_count = df[column].isna().sum()
            if null_count > 0:
                null_columns.append((column, null_count))
        
        assert len(null_columns) == 0, (
            f"Found null values in the following columns:\n" +
            "\n".join([f"  - {col}: {count} null values" for col, count in null_columns])
        )
