import pytest
import pandas as pd

import transform


@pytest.fixture
def df():
    return pd.read_csv('fixtures/fixture_1.csv')


def test_strip_leading_and_trailing_whitespace_from_column_names(df):
    assert df.columns[0] == 'Naive_Timestamp '
    transform.strip_leading_and_trailing_whitespace_from_column_names(df)
    assert df.columns[0] == 'Naive_Timestamp'


def test_rename_cols(df):
    assert df.columns[3] == 'Last Modified utc'
    transform.rename_cols(df)
    assert df.columns[3] == 'last_modified'


def test_cast_to_numeric_types_or_raise_exception_will_cast_values(df):
    transform.strip_leading_and_trailing_whitespace_from_column_names(df)
    transform.rename_cols(df)
    df.loc[0, 'value'] = "2"
    transform.cast_to_numeric_types(df)
    assert df.iloc[0].value == 2


def test_cast_to_numeric_types_or_raise_exception_will_raise_exception(df):
    transform.strip_leading_and_trailing_whitespace_from_column_names(df)
    transform.rename_cols(df)
    df.loc[0, 'value'] = "not_castable"
    transform.cast_to_numeric_types(df)
    assert df.iloc[0].value == 2
