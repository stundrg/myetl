import pandas as pd
import os
import sys


def load_data(input_path, output_path):
    """CSV 데이터를 읽어 Parquet으로 저장"""
    df = pd.read_csv(input_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow")
    print(f"✅ Parquet 파일 저장 완료: {output_path}")

if __name__ == "__main__":
    # TODO: 실제 실행 경로 지정
    input_csv = "/home/wsl/code/myetl/src/myetl/data.csv"  # 예: "/path/to/data.csv"
    output_parquet = "/home/wsl/code/myetl/src/myetl/data.parquet"  # 예: path/to/data.parquet
    load_data(input_csv, output_parquet)