import os

import pytest
import pandas as pd

from helpers import get_csv_schema
import transform


@pytest.fixture
def df():
    return pd.read_csv('fixtures/fixture_1.csv')


def test_validation_will_identify_faulty_file(df):
    df.loc[0, 'value'] = "invalid"
    schema = get_csv_schema()
    with pytest.raises(Exception):
        schema.validate(df)


def test_strip_leading_and_trailing_whitespace_from_column_names(df):
    assert df.columns[0] == 'Naive_Timestamp '
    transform.strip_leading_and_trailing_whitespace_from_column_names(df)
    assert df.columns[0] == 'Naive_Timestamp'


def test_rename_cols(df):
    assert df.columns[3] == 'Last Modified utc'
    transform.rename_cols(df)
    assert df.columns[3] == 'last_modified'


def test_normalise_timestamps(df):
    transform.strip_leading_and_trailing_whitespace_from_column_names(df)
    transform.rename_cols(df)
    assert str(df.loc[0, 'time_stamp']) == '2023-08-29 00:00:00'
    transform.normalise_timestamps(df, None)
    assert str(df.loc[0, 'time_stamp']) == '2023-08-29 00:00:00+00:00'


def test_append_chunk_to_csv_actually_populates_a_file_on_filesystem(df):
    transform.append_chunk_to_csv(df, True, "temp.csv")
    with open("temp.csv", "r") as file:
        contents = file.read()
        assert len(contents)
        assert 'Naive_Timestamp , Variable,value,Last Modified utc' in contents
    os.remove("temp.csv")
