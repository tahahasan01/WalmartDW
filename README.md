# Walmart Data Warehouse Project

Summary
-------
This repository contains a data warehouse project for a Walmart sales dataset. It includes SQL scripts to create the data warehouse schema, ETL code for loading dimensions and fact tables, an implementation of a HYBRIDJOIN algorithm used during loading, and a set of OLAP queries for analysis.

Key deliverables
----------------
- Create-DW.sql — SQL Server script to create the data warehouse schema (dimensions, fact table, and indexes).
- simple_loader.py — Production ETL script that loads customers, products, time dimension and the sales fact table.
- main.py — ETL orchestrator and helper routines.
- hybrid_join.py — HYBRIDJOIN algorithm implementation used in the ETL.
- Queries-DW.sql — A collection of OLAP queries for analytical testing and demonstration.
- Project-Report.txt — Project report with methodology, results and analysis.
- DIAGRAMS.md — Visual diagrams for schema, ETL flow and algorithm.

Optional / supporting files
--------------------------
- final_etl.py, fixed_etl.py — earlier ETL versions kept for traceability.
- CSV input files: customer_master_data.csv, product_master_data.csv, transactional_data/transactional_data.csv.
- SUMMARY.txt, README-COMPLETE.txt (may be present or modified).

Requirements
------------
- OS: Windows (tested here) but scripts are cross-platform where possible.
- Python: 3.8+ (development used 3.12).
- Packages: pandas, pyodbc.
- SQL Server: SQL Server 2016+ (tested on SQL Server 2019).
- ODBC Driver: Microsoft ODBC Driver 17 for SQL Server (or newer).

Quick setup
-----------
1. Ensure SQL Server is installed and running.
2. Install Python dependencies:

```powershell
python -m pip install pandas pyodbc
```

3. Create the database objects by executing Create-DW.sql in SQL Server Management Studio or with sqlcmd. This will create the WalmartDW schema and supporting objects.

Configuration
-------------
Open main.py or simple_loader.py and configure the connection string to your SQL Server instance. Example connection string (Windows integrated authentication):

```python
conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=WalmartDW;'
    'Trusted_Connection=yes;'
)
```

If you prefer SQL Server authentication, set UID and PWD in the connection string.

Loading data (recommended flow)
------------------------------
1. Execute Create-DW.sql to create schema and indexes.
2. Run the production loader (simple_loader.py) to populate dimensions and the fact table. The loader uses batch commits and in-memory lookups for better performance.

```powershell
# From the project directory
python .\simple_loader.py
```

Note: Loading the full transactional dataset may take significant time depending on machine specs. The loader prints progress updates and is designed to resume/retry on failures.

Running queries
---------------
After loading data, open Queries-DW.sql in SQL Server Management Studio and execute the batches to run the OLAP queries and review results.

Notes & tips
-----------
- If your Windows username contains spaces or special characters, using Trusted_Connection (Windows auth) is recommended for convenience.
- For large datasets, ensure SQL Server has sufficient memory and disk space for indexes and bulk inserts.
- The scripts were tested with SQL Server 2019; if you run into SQL compatibility issues, adapt or simplify the queries to match your server's supported features.

Reproducibility checklist
------------------------
- Create-DW.sql executed successfully.
- simple_loader.py run to completion and data loaded into the warehouse.
- Queries-DW.sql executed with results.
- Project report and diagrams reviewed for methodology and architecture.

Contributing & issues
---------------------
If you find bugs, have suggestions, or want to contribute improvements (examples: a requirements.txt, a run script, or additional documentation), please open an issue or a pull request on this repository.

Contact
-------
For questions or to report issues, please use the repository's GitHub Issues.

License
-------
This repository is provided for demonstration and learning. See LICENSE for details if present.

---

If you'd like an expanded README with step-by-step screenshots, or a requirements.txt and run script, tell me which to add and I will create them.