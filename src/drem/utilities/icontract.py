import pandas as pd


def columns_are_in_dataframe(columns, df):

    columns = set(columns)

    return columns.issubset(df.columns)


def error_rows_lost_in_merge(input_df, result_df):

    return (
        ValueError(
            f"Input DataFrame number of rows: {len(input_df)}"
            f" != Result DataFrame number of rows: {len(shape[0])}\n\n"
            " => some rows have been lost during the merge"
        ),
    )


def no_null_values_in_result_dataframe(result_df):

    return any(result_df.isna().any()) == False


def no_null_values_in_result_dataframe_columns(columns, result_df):

    result_df = result_df.reset_index()

    return result_df[columns].isna().any() == False


def error_null_values_in_result_dataframe(result_df):

    return ValueError(
        "Following 'True' columns contain null values:\n\n" f"{result_df.isna().any()}"
    )


def merge_column_dtypes_are_equivalent(left, right, merge_columns):

    return all(left[merge_columns].dtypes == right[merge_columns].dtypes)


def no_empty_values_in_merge_columns(merge_columns, df):

    return any(df[merge_columns].isnull().any())


def string_value_in_dataframe(to_find, df):

    return any(
        df.apply(
            lambda col: col.astype(str).str.contains(to_find).any(), axis="columns"
        )
    )
