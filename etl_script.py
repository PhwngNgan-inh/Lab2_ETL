import pandas as pd
import sqlite3
import os


def run_etl():
    print("STARTED ETL PROCESS...")

    # --- PHASE 1: EXTRACT ---
    print("[1] Extracting data from heterogeneous sources...")

    # Source 1: CSV
    try:
        df_customers = pd.read_csv("customers.csv")
        print(f"   -> Loaded {len(df_customers)} customers from CSV.")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Source 2: SQLite
    try:
        conn = sqlite3.connect("orders.db")
        df_orders = pd.read_sql_query("SELECT * FROM orders", conn)
        print(f"   -> Loaded {len(df_orders)} orders from SQLite.")
    except Exception as e:
        print(f"Error reading DB: {e}")
        return
    finally:
        if conn:
            conn.close()

    # --- PHASE 2: TRANSFORM ---
    print("[2] Transforming data...")

    merged_df = pd.merge(
        df_orders,
        df_customers,
        left_on="customer_id",
        right_on="id",
        how="left"
    )

    report_df = (
        merged_df
        .groupby(["name", "email"])["amount"]
        .sum()
        .reset_index()
    )

    report_df.columns = ["Customer Name", "Email", "Total Spent"]

    # --- CHALLENGE ---
    # Lọc khách hàng chi tiêu trên 500
    report_df = report_df[report_df["Total Spent"] > 500]

    # Sắp xếp theo số tiền giảm dần
    report_df = report_df.sort_values(by="Total Spent", ascending=False)

    # --- PHASE 3: LOAD ---
    print("[3] Loading Data to Report...")
    print("\n--- FINAL REPORT ---")
    print(report_df)

    report_df.to_csv("final_report.csv", index=False)
    print("\nReport saved to 'final_report.csv'")


if __name__ == "__main__":
    if not os.path.exists("orders.db"):
        import init_db

    run_etl()

