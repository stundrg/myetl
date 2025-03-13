import pandas as pd
import os
import sys


def load_data(input_path, output_path):
    df = pd.read_csv(input_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow")
    print(f"✅ Parquet 파일 저장 완료: {output_path}")

if __name__ == "__main__":
    input_csv = "/home/wsl/code/myetl/src/myetl/data.csv"  
    output_parquet = "/home/wsl/code/myetl/src/myetl/data.parquet"  
    load_data(input_csv, output_parquet)