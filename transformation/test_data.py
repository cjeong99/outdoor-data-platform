import pandas as pd
df = pd.read_parquet("data/bronze/recareas.parquet")
print(df.columns)
