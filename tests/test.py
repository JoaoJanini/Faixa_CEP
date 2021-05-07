from helpers import *
import pytest
def test_uppercase():
    assert "loud noises".upper() == "LOUD NOISES"

def test_reversed():
    assert list(reversed([1, 2, 3, 4])) == [4, 3, 2, 1]

def test_some_primes():
    assert 37 in {
        num
        for num in range(1, 50)
        if num != 1 and not any([num % div == 0 for div in range(2, num)])
    }
def test_get_proxy_list():
    assert get_proxy_list

def test_df_to_jsonl():
    assert df_to_jsonl()

def test_clean_data(df: pd.DataFrame):
    assert test_clean_data()

def test_make_post_request():
    assert 
def test_request_text_to_table():

def test_table_to_df()

def test_proxy_list_to_cycle():

def test_request_to_dataframe():