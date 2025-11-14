#!/usr/bin/env python3

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

sns.set(style="whitegrid")

# Paths
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR.parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input CSV paths (adjust if your files are in a different location)
TRANSACTIONS = INPUT_DIR / "transactional_data" / "transactional_data.csv"
PRODUCTS = INPUT_DIR / "product_master_data.csv"
CUSTOMERS = INPUT_DIR / "customer_master_data.csv"

# Helper to safely read CSVs
def read_csv_safe(path, parse_dates=None):
    if not path.exists():
        print(f"Warning: input file not found: {path}")
        return None
    try:
        return pd.read_csv(path, parse_dates=parse_dates)
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return None

def plot_sales_over_time(transactions):
    df = transactions.copy()
    # Ensure a datetime column exists (try common names)
    if 'transaction_date' in df.columns:
        df['date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    elif 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    else:
        print('No date column found for sales over time plot')
        return

    df = df.dropna(subset=['date'])
    df['month'] = df['date'].dt.to_period('M').dt.to_timestamp()

    if 'sales_amount' in df.columns:
        df['sales'] = pd.to_numeric(df['sales_amount'], errors='coerce')
    elif 'total' in df.columns:
        df['sales'] = pd.to_numeric(df['total'], errors='coerce')
    else:
        # Try to compute from quantity * price
        if 'quantity' in df.columns and 'price' in df.columns:
            df['sales'] = pd.to_numeric(df['quantity'], errors='coerce') * pd.to_numeric(df['price'], errors='coerce')
        else:
            print('No sales amount column found')
            return

    monthly = df.groupby('month')['sales'].sum().reset_index()

    plt.figure(figsize=(12,6))
    sns.lineplot(data=monthly, x='month', y='sales', marker='o')
    plt.title('Total Sales Over Time')
    plt.xlabel('Month')
    plt.ylabel('Sales')
    plt.tight_layout()
    out = OUTPUT_DIR / 'sales_over_time.png'
    plt.savefig(out)
    plt.close()
    print(f'Saved {out}')

def plot_sales_by_category(transactions, products):
    if products is None or 'product_id' not in products.columns:
        print('Products file missing or does not contain product_id. Skipping category plot.')
        return
    if 'product_id' not in transactions.columns:
        print('Transactions missing product_id. Skipping category plot.')
        return

    tx = transactions.merge(products[['product_id','category']], on='product_id', how='left')
    tx['sales'] = None
    if 'sales_amount' in tx.columns:
        tx['sales'] = pd.to_numeric(tx['sales_amount'], errors='coerce')
    elif 'total' in tx.columns:
        tx['sales'] = pd.to_numeric(tx['total'], errors='coerce')
    else:
        if 'quantity' in tx.columns and 'price' in tx.columns:
            tx['sales'] = pd.to_numeric(tx['quantity'], errors='coerce') * pd.to_numeric(tx['price'], errors='coerce')
        else:
            print('No sales amount available to compute category sales')
            return

    by_cat = tx.groupby('category')['sales'].sum().reset_index().sort_values('sales', ascending=False)
    plt.figure(figsize=(10,6))
    sns.barplot(data=by_cat.head(20), x='sales', y='category', palette='viridis')
    plt.title('Top Categories by Sales (Top 20)')
    plt.xlabel('Sales')
    plt.ylabel('Category')
    plt.tight_layout()
    out = OUTPUT_DIR / 'sales_by_category.png'
    plt.savefig(out)
    plt.close()
    print(f'Saved {out}')

def plot_top_products(transactions, products):
    if 'product_id' not in transactions.columns:
        print('Transactions missing product_id. Skipping top products plot.')
        return

    tx = transactions.copy()
    if 'quantity' in tx.columns:
        tx['quantity'] = pd.to_numeric(tx['quantity'], errors='coerce').fillna(0)
        top = tx.groupby('product_id')['quantity'].sum().reset_index().sort_values('quantity', ascending=False).head(20)
    else:
        print('No quantity column; attempting to use sales_amount/total')
        if 'sales_amount' in tx.columns:
            tx['sales'] = pd.to_numeric(tx['sales_amount'], errors='coerce')
        elif 'total' in tx.columns:
            tx['sales'] = pd.to_numeric(tx['total'], errors='coerce')
        else:
            print('No metric for top products found')
            return
        top = tx.groupby('product_id')['sales'].sum().reset_index().sort_values(top.columns[1], ascending=False).head(20)

    top = top.merge(products[['product_id','product_name']], on='product_id', how='left') if products is not None and 'product_name' in products.columns else top

    plt.figure(figsize=(10,8))
    sns.barplot(data=top, x=top.columns[1], y='product_name' if 'product_name' in top.columns else 'product_id', palette='rocket')
    plt.title('Top 20 Products')
    plt.xlabel(top.columns[1])
    plt.ylabel('Product')
    plt.tight_layout()
    out = OUTPUT_DIR / 'top_products.png'
    plt.savefig(out)
    plt.close()
    print(f'Saved {out}')

def plot_customers_by_region(customers):
    if customers is None:
        print('Customers file not provided. Skipping customer region plot.')
        return
    if 'region' not in customers.columns and 'state' not in customers.columns and 'city' not in customers.columns:
        print('No region/state/city column found in customers. Skipping.')
        return

    if 'region' in customers.columns:
        grp_col = 'region'
    elif 'state' in customers.columns:
        grp_col = 'state'
    else:
        grp_col = 'city'

    cnt = customers.groupby(grp_col).size().reset_index(name='count').sort_values('count', ascending=False).head(20)

    plt.figure(figsize=(10,6))
    sns.barplot(data=cnt, x='count', y=grp_col, palette='magma')
    plt.title(f'Customers by {grp_col.title()} (Top 20)')
    plt.xlabel('Number of Customers')
    plt.ylabel(grp_col.title())
    plt.tight_layout()
    out = OUTPUT_DIR / f'customers_by_{grp_col}.png'
    plt.savefig(out)
    plt.close()
    print(f'Saved {out}')


def main():
    transactions = read_csv_safe(TRANSACTIONS, parse_dates=['transaction_date', 'date'])
    products = read_csv_safe(PRODUCTS)
    customers = read_csv_safe(CUSTOMERS)

    if transactions is None and products is None and customers is None:
        print('No input files found. Place CSVs in the project root as described in visuals/README.md')
        return

    if transactions is not None:
        plot_sales_over_time(transactions)
        plot_top_products(transactions, products)
        plot_sales_by_category(transactions, products)
    if customers is not None:
        plot_customers_by_region(customers)

    print('All available visuals generated in:', OUTPUT_DIR)

if __name__ == '__main__':
    main()