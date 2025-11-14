-- ==============================================================================
-- Data Warehouse Schema Creation Script for Walmart Near-Real-Time DW
-- Course: DS3003 & DS3004 - Data Warehousing & Business Intelligence
-- Project: Building and Analysing a Near-Real-Time Data Warehouse
-- ==============================================================================

-- Drop existing schema if it exists
-- IMPORTANT: Drop in correct order to avoid foreign key constraint violations
-- 1. Drop materialized view first
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'STORE_QUARTERLY_SALES')
    DROP TABLE STORE_QUARTERLY_SALES;

-- 2. Drop fact table (has foreign keys)
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'SALES_FACT')
    DROP TABLE SALES_FACT;

-- 3. Drop dimension tables (no dependencies after fact table is gone)
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DIM_TIME')
    DROP TABLE DIM_TIME;

IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DIM_SUPPLIER')
    DROP TABLE DIM_SUPPLIER;

IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DIM_STORE')
    DROP TABLE DIM_STORE;

IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DIM_PRODUCT')
    DROP TABLE DIM_PRODUCT;

IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DIM_CUSTOMER')
    DROP TABLE DIM_CUSTOMER;

-- ==============================================================================
-- DIMENSION TABLES
-- ==============================================================================

-- 1. CUSTOMER DIMENSION TABLE
CREATE TABLE DIM_CUSTOMER (
    Customer_SK INT PRIMARY KEY IDENTITY(1,1),
    Customer_ID VARCHAR(20) NOT NULL UNIQUE,
    Gender CHAR(1),
    Age_Group VARCHAR(10),
    Age_Numeric INT,
    Occupation INT,
    City_Category CHAR(1),
    Stay_In_Current_City_Years INT,
    Marital_Status INT,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);

-- 2. PRODUCT DIMENSION TABLE
CREATE TABLE DIM_PRODUCT (
    Product_SK INT PRIMARY KEY IDENTITY(1,1),
    Product_ID VARCHAR(20) NOT NULL UNIQUE,
    Product_Category VARCHAR(50),
    Price DECIMAL(10, 2),
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);

-- 3. STORE DIMENSION TABLE
CREATE TABLE DIM_STORE (
    Store_SK INT PRIMARY KEY IDENTITY(1,1),
    StoreID INT NOT NULL UNIQUE,
    StoreName VARCHAR(100),
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);

-- 4. SUPPLIER DIMENSION TABLE
CREATE TABLE DIM_SUPPLIER (
    Supplier_SK INT PRIMARY KEY IDENTITY(1,1),
    SupplierID INT NOT NULL UNIQUE,
    SupplierName VARCHAR(100),
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);

-- 5. TIME DIMENSION TABLE
CREATE TABLE DIM_TIME (
    Date_SK INT PRIMARY KEY IDENTITY(1,1),
    TransactionDate DATE NOT NULL UNIQUE,
    Year INT,
    Month INT,
    MonthName VARCHAR(20),
    Quarter INT,
    Day INT,
    DayOfWeek INT,
    DayName VARCHAR(20),
    WeekOfYear INT,
    Season VARCHAR(10),
    IsWeekend INT,
    IsHoliday INT
);

-- ==============================================================================
-- FACT TABLE
-- ==============================================================================

CREATE TABLE SALES_FACT (
    Sales_SK INT PRIMARY KEY IDENTITY(1,1),
    OrderID VARCHAR(20) NOT NULL,
    Customer_SK INT NOT NULL,
    Product_SK INT NOT NULL,
    Store_SK INT NOT NULL,
    Supplier_SK INT NOT NULL,
    Date_SK INT NOT NULL,
    Quantity INT,
    UnitPrice DECIMAL(10, 2),
    TotalAmount DECIMAL(12, 2),
    CreatedDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (Customer_SK) REFERENCES DIM_CUSTOMER(Customer_SK),
    FOREIGN KEY (Product_SK) REFERENCES DIM_PRODUCT(Product_SK),
    FOREIGN KEY (Store_SK) REFERENCES DIM_STORE(Store_SK),
    FOREIGN KEY (Supplier_SK) REFERENCES DIM_SUPPLIER(Supplier_SK),
    FOREIGN KEY (Date_SK) REFERENCES DIM_TIME(Date_SK)
);

-- ==============================================================================
-- INDEXES FOR PERFORMANCE OPTIMIZATION
-- ==============================================================================

CREATE INDEX IX_SALES_FACT_CUSTOMER ON SALES_FACT(Customer_SK);
CREATE INDEX IX_SALES_FACT_PRODUCT ON SALES_FACT(Product_SK);
CREATE INDEX IX_SALES_FACT_STORE ON SALES_FACT(Store_SK);
CREATE INDEX IX_SALES_FACT_SUPPLIER ON SALES_FACT(Supplier_SK);
CREATE INDEX IX_SALES_FACT_DATE ON SALES_FACT(Date_SK);
CREATE INDEX IX_SALES_FACT_ORDER ON SALES_FACT(OrderID);

CREATE INDEX IX_DIM_CUSTOMER_ID ON DIM_CUSTOMER(Customer_ID);
CREATE INDEX IX_DIM_PRODUCT_ID ON DIM_PRODUCT(Product_ID);
CREATE INDEX IX_DIM_STORE_ID ON DIM_STORE(StoreID);
CREATE INDEX IX_DIM_SUPPLIER_ID ON DIM_SUPPLIER(SupplierID);
CREATE INDEX IX_DIM_TIME_DATE ON DIM_TIME(TransactionDate);

-- ==============================================================================
-- MATERIALIZED VIEW FOR QUARTERLY SALES ANALYSIS (Q20)
-- ==============================================================================

CREATE TABLE STORE_QUARTERLY_SALES (
    Store_SK INT NOT NULL,
    StoreName VARCHAR(100),
    Year INT,
    Quarter INT,
    TotalSales DECIMAL(12, 2),
    TotalQuantity INT,
    TransactionCount INT,
    PRIMARY KEY (Store_SK, Year, Quarter),
    FOREIGN KEY (Store_SK) REFERENCES DIM_STORE(Store_SK)
);

CREATE INDEX IX_STORE_QUARTERLY_SALES ON STORE_QUARTERLY_SALES(StoreName, Year, Quarter);

-- ==============================================================================
-- SCHEMA CREATION COMPLETE
-- ==============================================================================

PRINT 'Star Schema created successfully!'
PRINT 'Dimension Tables: DIM_CUSTOMER, DIM_PRODUCT, DIM_STORE, DIM_SUPPLIER, DIM_TIME'
PRINT 'Fact Table: SALES_FACT'
PRINT 'Materialized View: STORE_QUARTERLY_SALES'
