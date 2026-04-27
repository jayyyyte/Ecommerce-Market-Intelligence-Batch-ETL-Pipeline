import pandas as pd
from etl.transformer import DataTransformer
from etl.config import get_config
import uuid

cfg = get_config()
transformer = DataTransformer(cfg)

# Load the real staging file
staging_file = "staging/2026-04-27/raw_products.json"  # adjust date
raw_df = pd.read_json(staging_file)

# Transform
run_id = str(uuid.uuid4())
clean_df, rejected_df, quality_score = transformer.transform(raw_df, run_id)

print(f"Quality Score: {quality_score}%")
print(f"Clean rows: {len(clean_df)}")
print(f"Rejected rows: {len(rejected_df)}")
print(f"\nClean columns: {list(clean_df.columns)}")
print(f"\nSample clean row:\n{clean_df.iloc[0]}")

if not rejected_df.empty:
    print(f"\nRejection reasons:\n{rejected_df['rejection_reason'].value_counts()}")