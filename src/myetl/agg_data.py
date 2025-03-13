import pandas as pd
import os
import sys

def agg_data(input_path, output_path):
    """Parquet 데이터를 읽고 name, value로 그룹화 후 CSV 저장"""
    df = pd.read_parquet(input_path, engine="pyarrow")
    agg_df = df.groupby(['name', 'value']).size().reset_index(name='count')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    agg_df.to_csv(output_path, index=False)
    print(f"✅ Aggregated CSV 저장 완료: {output_path}")

if __name__ == "__main__":
    # ✅ TODO: 실제 실행 경로 입력
    input_parquet = "/home/wsl/code/myetl/src/myetl/data.parquet"  
    output_csv = "~/data/seoul/{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}" 
    agg_data(input_parquet, output_csv)