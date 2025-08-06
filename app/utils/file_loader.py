import os
import pandas as pd
from app.db import get_connection

async def save_csv(folder_path: str, table_name: str = "FinalCSV"):
    conn = get_connection()
    cursor = conn.cursor()

    merged_df = None

    for root, dirnames, filenames in os.walk(folder_path):
      for file in filenames:
        if file.endswith(".csv"):
            try:
                df = pd.read_csv(os.path.join(root, file))
                if not df.empty:
                    merged_df = pd.concat([merged_df, df], ignore_index=True) if merged_df is not None else df
            except Exception as e:
                return f"Error reading {file}: {e}"
    if merged_df is None or merged_df.empty:
        return "No valid CSV data found."
    

    merged_df.fillna("", inplace=True)
    columns = merged_df.columns
    column_defs = ", ".join(f"[{col}] NVARCHAR(MAX)" for col in columns)

    cursor.execute(f"""
            CREATE TABLE [{table_name}] ({column_defs})
    """)
    conn.commit()

    insert_stmt = f"INSERT INTO [{table_name}] ({', '.join(f'[{col}]' for col in columns)}) VALUES ({', '.join(['?'] * len(columns))})"
    data = [tuple(row.astype(str)) for index, row in merged_df.iterrows()]


    try:
        cursor.executemany(insert_stmt, data)
    except Exception as e:
        return f"Insert error: {e}"

    conn.commit()
    cursor.close()
    conn.close()
    return f"Inserted {len(merged_df)} rows into [{table_name}]"
