""""Test function pytest"""
import pytest
import pandas as pd
from unittest.mock import MagicMock


def merge_dataframes_on_siren(df_inpi, df_insee):
    """Merge two DataFrames on the 'siren' column."""
    return pd.merge(df_inpi, df_insee, on="siren", how="inner")


def fetch_api_data(api_client, query):
    """Simulated API fetch function for test purposes."""
    response = api_client.get(query)
    if response.status_code != 200:
        raise Exception("API Request failed")
    return response.json()


def test_merge_dataframes_on_siren():
    """Test merge."""
    df_inpi = pd.DataFrame({
        'siren': ['111', '222', '333'],
        'val_inpi': [10, 20, 30]
    })
    df_insee = pd.DataFrame({
        'siren': ['111', '222', '444'],
        'val_insee': ['A', 'B', 'C']
    })
    merged = merge_dataframes_on_siren(df_inpi, df_insee)
    assert set(merged['siren']) == {'111', '222'}
    assert 'val_inpi' in merged.columns
    assert 'val_insee' in merged.columns


def test_fetch_api_data_success():
    """Test fetch."""
    mock_api_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'result': 'ok'}
    mock_api_client.get.return_value = mock_response

    result = fetch_api_data(mock_api_client, 'some_query')
    assert result == {'result': 'ok'}


def test_fetch_api_data_failure():
    """Test fetch failure."""
    mock_api_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_api_client.get.return_value = mock_response

    with pytest.raises(Exception):
        fetch_api_data(mock_api_client, 'bad_query')
