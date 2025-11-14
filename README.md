# Walmart Data Warehouse Project (i211767)

Summary
-------
This repository contains a complete data warehouse project for the Walmart dataset. It includes schema creation, ETL code (production loader), a HYBRIDJOIN algorithm implementation, and 20 OLAP queries. All deliverables required for the course submission are provided and documented.

Key Deliverables
----------------
- `Create-DW.sql` — SQL Server script to create the data warehouse schema (dimensions, fact, indexes).
- `simple_loader.py` — Production ETL script that loads customers, products, time dimension and the sales fact table.
- `main.py` — ETL orchestrator and helper routines.
- `hybrid_join.py` — HYBRIDJOIN algorithm implementation used in the ETL.
- `Queries-DW.sql` — 20 OLAP queries for analysis and marking.
- `Project-Report.txt` — Full project report with methodology, results and analysis.
- `DIAGRAMS.md` — Visual diagrams for schema, ETL flow and algorithm.

Optional / Supporting Files
--------------------------
- `final_etl.py`, `fixed_etl.py` — earlier ETL versions (kept for traceability).
- CSV input files: `customer_master_data.csv`, `product_master_data.csv`, `transactional_data/transactional_data.csv`.
- `SUMMARY.txt`, `README-COMPLETE.txt`, `SUBMISSION-GUIDE.txt` (may be present or modified).

Requirements
------------
- OS: Windows (tested here)
- Python: 3.8+ (3.12 used during development)
- Packages: `pandas`, `pyodbc`
- SQL Server: SQL Server 2016+ (tested on SQL Server 2019)
- ODBC Driver: Microsoft ODBC Driver 17 for SQL Server (or newer)

Quick Setup
-----------
1. Ensure SQL Server is installed and running.
2. Install Python dependencies:

```powershell
python -m pip install pandas pyodbc
```

3. Open `Create-DW.sql` in SQL Server Management Studio (or run with sqlcmd) and execute to create the `WalmartDW` schema and indexes.

4. Configure authentication in `main.py` or `simple_loader.py` connection string (the code auto-detects Windows auth when password is empty). Example connection string used by scripts:

```python
conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=WalmartDW;'
    'Trusted_Connection=yes;'
)
```

Loading Data (recommended flow)
------------------------------
1. Create the database objects by executing `Create-DW.sql`.
2. Run the production loader (`simple_loader.py`) to populate dimensions and facts. This script uses batch commits and in-memory lookups for performance.

```powershell
# From the project directory
python .\simple_loader.py
```

Note: Loading all transactions may take ~60–90 minutes depending on machine specs. The script prints progress periodically.

Running Queries
---------------
After data is loaded, open `Queries-DW.sql` in SQL Server Management Studio and execute the batches to see the results for the 20 OLAP queries.

Project Notes & Tips
--------------------
- If your Windows username contains spaces, the loader defaults to using `Trusted_Connection=yes`.
- If you prefer SQL authentication, update the `UID` and `PWD` in the connection string in `main.py` or `simple_loader.py`.
- If queries use window functions or advanced constructs and your SQL Server is older, adjust SQL accordingly (scripts tested on SQL Server 2019).

Reproducibility Checklist
------------------------
- [ ] `Create-DW.sql` executed successfully
- [ ] `simple_loader.py` run to completion (550,068 transactions loaded)
- [ ] `Queries-DW.sql` executed with results
- [ ] `Project-Report.txt` and `DIAGRAMS.md` reviewed

If You Need To Submit
---------------------
Create a folder `Taha_i211767_Project` containing the 5 required files plus README and diagrams, compress it, and upload to your submission system.

Example PowerShell compress command:

```powershell
Compress-Archive -Path "Taha_i211767_Project" -DestinationPath "Taha_i211767_Project.zip"
```

Contact
-------
For questions about the repository or reproducibility, contact:
- Student: Syed Taha Hasan (i211767)

License
-------
This repository is for course submission and academic use only.

---

If you'd like a longer README with step-by-step screenshots, or a `requirements.txt` + run script, tell me which to add and I will create them.