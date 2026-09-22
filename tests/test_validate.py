import pandas as pd
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "quality_checks"))

from validate import (
    DataQualityError,
    check_no_nulls,
    check_no_duplicates,
    check_positive,
    check_referential_integrity,
)


def test_check_no_nulls_raises_on_null():
    df = pd.DataFrame({"user_id": [1, 2, None]})
    with pytest.raises(DataQualityError):
        check_no_nulls(df, ["user_id"], "test")


def test_check_no_nulls_passes_clean_data():
    df = pd.DataFrame({"user_id": [1, 2, 3]})
    check_no_nulls(df, ["user_id"], "test")  # не должно бросить исключение


def test_check_no_duplicates_raises_on_dupe():
    df = pd.DataFrame({"order_id": [1, 1, 2]})
    with pytest.raises(DataQualityError):
        check_no_duplicates(df, "order_id", "test")


def test_check_positive_raises_on_zero_or_negative():
    df = pd.DataFrame({"quantity": [1, 0, -2]})
    with pytest.raises(DataQualityError):
        check_positive(df, "quantity", "test")


def test_check_referential_integrity_raises_on_orphan():
    child = pd.DataFrame({"user_id": [1, 2, 999]})
    parent_ids = {1, 2, 3}
    with pytest.raises(DataQualityError):
        check_referential_integrity(child, "user_id", parent_ids, "test")


def test_check_referential_integrity_passes_when_all_valid():
    child = pd.DataFrame({"user_id": [1, 2, 3]})
    parent_ids = {1, 2, 3}
    check_referential_integrity(child, "user_id", parent_ids, "test")
