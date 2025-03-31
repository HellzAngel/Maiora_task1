import pandas as pd
import sqlite3
import json

# File paths
file_a = "order_region_a.csv"
file_b = "order_region_b.csv"

def read_csv(file_path, region):
    """ Reads CSV data and adds a region column """
    df = pd.read_csv(file_path)
    df["region"] = region  # Add region identifier
    return df

def clean_and_transform(df):
    """ Cleans and applies business rules to transform data """
    # Standardize column names
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    #converting to string
    df["orderitemid"] = df["orderitemid"].astype(str)

    # Extract discount amount from PromotionDiscount
    df["promotion_discount"] = df["promotiondiscount"].apply(
        lambda x: float(json.loads(x)["Amount"]) if pd.notna(x) else 0
    )

    # Calculate total sales
    df["total_sales"] = df["quantityordered"] * df["itemprice"]

    # Calculate net sales
    df["net_sale"] = df["total_sales"] - df["promotion_discount"]

    # Remove duplicate OrderIds (keeping the first occurrence)
    df = df.drop_duplicates(subset="orderid", keep="first")

    # Remove records where net_sale is negative or zero
    df = df[df["net_sale"] > 0]

    return df

def load_to_db(df, db_name="sales_data.db", table_name="sales_data"):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

def validate_data(db_name="sales_data.db"):
    """ Validates data with SQL queries """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    queries = {
        "Total Records": "SELECT COUNT(*) FROM sales_data;",
        "Total Sales by Region": "SELECT region, SUM(total_sales) FROM sales_data GROUP BY region;",
        "Average Sales per Transaction": "SELECT AVG(total_sales) FROM sales_data;",
        "duplicate orderid ": "SELECT orderid, COUNT(*) FROM sales_data GROUP BY orderid HAVING COUNT(*) > 1;"
        
    }

    for desc, query in queries.items():
        cursor.execute(query)
        print(f"{desc}: {cursor.fetchall()}")

    conn.close()

#1 . Extract data from csv files
df_a = read_csv(file_a, "A")
df_b = read_csv(file_b, "B")
#print(df_a)
#print(df_b)

#2. Transform the data based on rules
df_a = clean_and_transform(df_a)
df_b = clean_and_transform(df_b)
#print(df_a)

# Combine both dataset 

df_combined = pd.concat([df_a, df_b])

#cleaning the combined df having only unique orderid
df_combined = df_combined.drop_duplicates(subset="orderid", keep="first")

#df_combined = df_combined.groupby("orderid", group_keys=False).sample(n=1).reset_index(drop=True)

#print(df_combined)

# Load to database
load_to_db(df_combined)

# Validate data
validate_data()
