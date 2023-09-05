from datetime import datetime
from unittest import mock

import helpers


@mock.patch('helpers.datetime')
def test_get_latest_dates(mock_datetime):
    """We expect to see a list of strftimes with the following format."""
    mock_datetime.now.return_value = datetime(2023, 8, 5)
    expected = ['2023-07-28', '2023-07-29', '2023-07-30', '2023-07-31', '2023-08-01', '2023-08-02', '2023-08-03', '2023-08-04', '2023-08-05']
    actual = helpers.get_latest_dates()
    assert expected == actual


@mock.patch('helpers.datetime')
def test_construct_raw_tasks_list(mock_datetime):
    mock_datetime.now.return_value = datetime(2023, 8, 5)
    dates = helpers.get_latest_dates()[:1]
    actual = helpers.construct_raw_tasks_list(dates)
    assert actual[0]['input_data_url'] == '/2023-07-28/renewables/solargen.json'