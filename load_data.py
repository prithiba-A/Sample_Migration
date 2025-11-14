import pandas as pd
from sqlalchemy import create_engine
import os

print("🚀 Starting CSV → PostgreSQL Loader...")

# Railway injects these automatically
PGUSER     = os.getenv("PGUSER")
PGPASSWORD = os.getenv("POSTGRES_PASSWORD")
PGDATABASE = os.getenv("PGDATABASE")
PGHOST     = os.getenv("RAILWAY_TCP_PROXY_DOMAIN")
PGPORT     = os.getenv("RAILWAY_TCP_PROXY_PORT")

db_url = f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
engine = create_engine(db_url)

datasets = {
    "pdms_history_large.csv": "pdms_history",
    "opc_scada_large.csv": "opc_scada",
    "steel_process_large.csv": "steel_process",
    "ems_energy_large.csv": "ems_energy",
    "cement_lab_large.csv": "cement_lab",
    "air_quality_large.csv": "air_quality"
}

for csv, table in datasets.items():
    print(f"📥 Loading {csv} → {table} ...")

    df = pd.read_csv(csv)

    if "timestamp" in df.columns:
        df.rename(columns={"timestamp": "ts"}, inplace=True)

    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"])

    df.to_sql(table, engine, if_exists="replace", index=False)

    print(f"✅ Done {table}")

print("🎉 ALL DATASETS IMPORTED SUCCESSFULLY!")
