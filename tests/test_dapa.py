import pandas as pd
import pytest
import os
from myetl.load_data import load_data
from myetl.agg_data import agg_data


@pytest.fixture
def temp_files(tmp_path):
    """테스트에 필요한 임시 파일 경로를 미리 설정"""
    files = {
        "csv": tmp_path / "test.csv",
        "parquet": tmp_path / "test.parquet",
        "agg_csv": tmp_path / "agg.csv"
    }
    return files


def test_load_data(temp_files):
    """CSV → Parquet 변환 테스트"""
    df = pd.DataFrame({'name': ['A', 'B', 'A'], 'value': [1, 2, 1]})
    df.to_csv(temp_files["csv"], index=False)

    load_data(temp_files["csv"], temp_files["parquet"])

    assert temp_files["parquet"].exists()


def test_agg_data(temp_files):
    """Parquet → Aggregation 후 CSV 변환 테스트"""
    df = pd.DataFrame({'name': ['A', 'B', 'A'], 'value': [1, 2, 1]})
    df.to_parquet(temp_files["parquet"], engine="pyarrow")

    agg_data(temp_files["parquet"], temp_files["agg_csv"])

    assert temp_files["agg_csv"].exists()


if __name__ == "__main__":
    pytest.main()
