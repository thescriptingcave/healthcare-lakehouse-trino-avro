import pandas as pd
from trino.dbapi import connect

# ----------------------------
# 1. Load CSV
# ----------------------------
df = pd.read_csv("./fdny_firehouse_listing.csv")
df.columns = [c.replace(" ", "_").replace("/", "_").lower() for c in df.columns]
rows = df.to_numpy().tolist()

# ----------------------------
# 2. Connect to Trino
# ----------------------------
conn = connect(
    host="127.0.0.1",
    port=8082,
    user="trino",
    catalog="minio",
    schema="healthcare",
    http_scheme="http",
)

cur = conn.cursor()
print("Connected as user: trino")

# ----------------------------
# 3. DROP & CREATE (FULLY QUALIFIED)
# ----------------------------
table_name = "minio.healthcare.fdny_firehouse_silver"

cur.execute(f"DROP TABLE IF EXISTS {table_name}")

columns_sql = ", ".join(f"{col} VARCHAR" for col in df.columns)

cur.execute(
    f"""
    CREATE TABLE {table_name} (
        {columns_sql}
    )
    WITH (
        format = 'PARQUET'
    )
    """
)

print("Table created")

# ----------------------------
# 4. SQL-safe helper
# ----------------------------
def sql_value(v):
    if pd.isna(v):
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"

# ----------------------------
# 5. Batch insert (small batches!)
# ----------------------------
batch_size = 50  # keep small
total = len(rows)

for i in range(0, total, batch_size):
    batch = rows[i:i + batch_size]

    values_sql = ", ".join(
        "(" + ", ".join(sql_value(v) for v in row) + ")"
        for row in batch
    )

    cur.execute(
        f"""
        INSERT INTO {table_name}
        VALUES {values_sql}
        """
    )

    print(f"Inserted {i + len(batch)} / {total}")

conn.close()
print("--- SUCCESS ---")
