-- ==============================================================================
-- OLAP Queries for Walmart Data Warehouse Analysis
-- Course: DS3003 & DS3004 - Data Warehousing & Business Intelligence
-- ==============================================================================

-- ======================================
-- Q1: Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
-- ======================================
SELECT TOP 5
    P.Product_Category,
    P.Product_ID,
    P.Price,
    CASE WHEN DT.IsWeekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS DayType,
    YEAR(DT.TransactionDate) AS Year,
    MONTH(DT.TransactionDate) AS Month,
    SUM(SF.TotalAmount) AS TotalRevenue,
    COUNT(*) AS TransactionCount
FROM SALES_FACT SF
JOIN DIM_PRODUCT P ON SF.Product_SK = P.Product_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
GROUP BY 
    P.Product_Category,
    P.Product_ID,
    P.Price,
    CASE WHEN DT.IsWeekend = 1 THEN 'Weekend' ELSE 'Weekday' END,
    YEAR(DT.TransactionDate),
    MONTH(DT.TransactionDate)
ORDER BY TotalRevenue DESC;

-- ======================================
-- Q2: Customer Demographics by Purchase Amount with City Category Breakdown
-- ======================================
SELECT
    DC.Gender,
    DC.Age_Group,
    DC.City_Category,
    COUNT(DISTINCT DC.Customer_SK) AS CustomerCount,
    SUM(SF.TotalAmount) AS TotalPurchaseAmount,
    AVG(SF.TotalAmount) AS AvgPurchaseAmount,
    COUNT(*) AS TransactionCount
FROM SALES_FACT SF
JOIN DIM_CUSTOMER DC ON SF.Customer_SK = DC.Customer_SK
GROUP BY 
    DC.Gender,
    DC.Age_Group,
    DC.City_Category
ORDER BY TotalPurchaseAmount DESC;

-- ======================================
-- Q3: Product Category Sales by Occupation
-- ======================================
SELECT
    DC.Occupation,
    P.Product_Category,
    SUM(SF.Quantity) AS TotalQuantity,
    SUM(SF.TotalAmount) AS TotalSales,
    AVG(SF.TotalAmount) AS AvgSaleValue,
    COUNT(DISTINCT DC.Customer_SK) AS CustomerCount
FROM SALES_FACT SF
JOIN DIM_CUSTOMER DC ON SF.Customer_SK = DC.Customer_SK
JOIN DIM_PRODUCT P ON SF.Product_SK = P.Product_SK
GROUP BY 
    DC.Occupation,
    P.Product_Category
ORDER BY TotalSales DESC;

-- ======================================
-- Q4: Total Purchases by Gender and Age Group with Quarterly Trend
-- ======================================
SELECT
    DC.Gender,
    DC.Age_Group,
    YEAR(DT.TransactionDate) AS Year,
    DT.Quarter,
    SUM(SF.Quantity) AS TotalQuantity,
    SUM(SF.TotalAmount) AS TotalPurchaseAmount,
    AVG(SF.TotalAmount) AS AvgPurchaseAmount,
    COUNT(*) AS TransactionCount
FROM SALES_FACT SF
JOIN DIM_CUSTOMER DC ON SF.Customer_SK = DC.Customer_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
WHERE YEAR(DT.TransactionDate) = YEAR(GETDATE())
GROUP BY 
    DC.Gender,
    DC.Age_Group,
    YEAR(DT.TransactionDate),
    DT.Quarter
ORDER BY Year, Quarter, DC.Gender, DC.Age_Group;

-- ======================================
-- Q5: Top Occupations by Product Category Sales
-- ======================================
SELECT TOP 5
    DC.Occupation,
    P.Product_Category,
    SUM(SF.TotalAmount) AS TotalSales,
    COUNT(*) AS TransactionCount,
    AVG(SF.TotalAmount) AS AvgSaleValue
FROM SALES_FACT SF
JOIN DIM_CUSTOMER DC ON SF.Customer_SK = DC.Customer_SK
JOIN DIM_PRODUCT P ON SF.Product_SK = P.Product_SK
GROUP BY 
    DC.Occupation,
    P.Product_Category
ORDER BY TotalSales DESC;

-- ======================================
-- Q6: City Category Performance by Marital Status with Monthly Breakdown
-- ======================================
SELECT
    DC.City_Category,
    DC.Marital_Status,
    YEAR(DT.TransactionDate) AS Year,
    MONTH(DT.TransactionDate) AS Month,
    SUM(SF.TotalAmount) AS TotalPurchaseAmount,
    COUNT(*) AS TransactionCount,
    AVG(SF.TotalAmount) AS AvgPurchaseAmount
FROM SALES_FACT SF
JOIN DIM_CUSTOMER DC ON SF.Customer_SK = DC.Customer_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
WHERE DATEDIFF(MONTH, DT.TransactionDate, GETDATE()) <= 6
GROUP BY 
    DC.City_Category,
    DC.Marital_Status,
    YEAR(DT.TransactionDate),
    MONTH(DT.TransactionDate)
ORDER BY Year DESC, Month DESC, TotalPurchaseAmount DESC;

-- ======================================
-- Q7: Average Purchase Amount by Stay Duration and Gender
-- ======================================
SELECT
    DC.Stay_In_Current_City_Years AS StayDurationYears,
    DC.Gender,
    COUNT(DISTINCT DC.Customer_SK) AS CustomerCount,
    AVG(SF.TotalAmount) AS AvgPurchaseAmount,
    SUM(SF.TotalAmount) AS TotalAmount,
    COUNT(*) AS TransactionCount
FROM SALES_FACT SF
JOIN DIM_CUSTOMER DC ON SF.Customer_SK = DC.Customer_SK
GROUP BY 
    DC.Stay_In_Current_City_Years,
    DC.Gender
ORDER BY StayDurationYears, Gender;

-- ======================================
-- Q8: Top 5 Revenue-Generating Cities by Product Category
-- ======================================
SELECT TOP 5
    DC.City_Category,
    P.Product_Category,
    SUM(SF.TotalAmount) AS TotalRevenue,
    SUM(SF.Quantity) AS TotalQuantity,
    COUNT(DISTINCT DC.Customer_SK) AS UniqueCustomers,
    AVG(SF.TotalAmount) AS AvgSaleValue
FROM SALES_FACT SF
JOIN DIM_CUSTOMER DC ON SF.Customer_SK = DC.Customer_SK
JOIN DIM_PRODUCT P ON SF.Product_SK = P.Product_SK
GROUP BY 
    DC.City_Category,
    P.Product_Category
ORDER BY TotalRevenue DESC;

-- ======================================
-- Q9: Monthly Sales Growth by Product Category
-- ======================================
SELECT
    P.Product_Category,
    YEAR(DT.TransactionDate) AS Year,
    MONTH(DT.TransactionDate) AS Month,
    SUM(SF.TotalAmount) AS MonthlySales,
    LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY P.Product_Category ORDER BY YEAR(DT.TransactionDate), MONTH(DT.TransactionDate)) AS PreviousMonthlySales,
    CASE 
        WHEN LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY P.Product_Category ORDER BY YEAR(DT.TransactionDate), MONTH(DT.TransactionDate)) IS NULL THEN 0
        ELSE ROUND(((SUM(SF.TotalAmount) - LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY P.Product_Category ORDER BY YEAR(DT.TransactionDate), MONTH(DT.TransactionDate))) 
            / LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY P.Product_Category ORDER BY YEAR(DT.TransactionDate), MONTH(DT.TransactionDate))) * 100, 2)
    END AS GrowthPercentage
FROM SALES_FACT SF
JOIN DIM_PRODUCT P ON SF.Product_SK = P.Product_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
WHERE YEAR(DT.TransactionDate) = YEAR(GETDATE())
GROUP BY 
    P.Product_Category,
    YEAR(DT.TransactionDate),
    MONTH(DT.TransactionDate)
ORDER BY Year, Month;

-- ======================================
-- Q10: Weekend vs. Weekday Sales by Age Group
-- ======================================
SELECT
    DC.Age_Group,
    CASE WHEN DT.IsWeekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS DayType,
    SUM(SF.TotalAmount) AS TotalSales,
    COUNT(*) AS TransactionCount,
    AVG(SF.TotalAmount) AS AvgSaleValue,
    SUM(SF.Quantity) AS TotalQuantity
FROM SALES_FACT SF
JOIN DIM_CUSTOMER DC ON SF.Customer_SK = DC.Customer_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
WHERE YEAR(DT.TransactionDate) = YEAR(GETDATE())
GROUP BY 
    DC.Age_Group,
    CASE WHEN DT.IsWeekend = 1 THEN 'Weekend' ELSE 'Weekday' END
ORDER BY DC.Age_Group, DayType;

-- ======================================
-- Q11: Top 5 Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
-- ======================================
SELECT TOP 5
    P.Product_ID,
    P.Product_Category,
    CASE WHEN DT.IsWeekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS DayType,
    YEAR(DT.TransactionDate) AS Year,
    MONTH(DT.TransactionDate) AS Month,
    SUM(SF.TotalAmount) AS TotalRevenue,
    SUM(SF.Quantity) AS TotalQuantity,
    COUNT(*) AS TransactionCount
FROM SALES_FACT SF
JOIN DIM_PRODUCT P ON SF.Product_SK = P.Product_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
WHERE YEAR(DT.TransactionDate) = 2017
GROUP BY 
    P.Product_ID,
    P.Product_Category,
    CASE WHEN DT.IsWeekend = 1 THEN 'Weekend' ELSE 'Weekday' END,
    YEAR(DT.TransactionDate),
    MONTH(DT.TransactionDate)
ORDER BY TotalRevenue DESC;

-- ======================================
-- Q12: Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
-- ======================================
SELECT
    DS.StoreID,
    DS.StoreName,
    DT.Quarter,
    SUM(SF.TotalAmount) AS QuarterlyRevenue,
    LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY DS.StoreID ORDER BY DT.Quarter) AS PreviousQuarterRevenue,
    CASE 
        WHEN LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY DS.StoreID ORDER BY DT.Quarter) IS NULL THEN 0
        ELSE ROUND(((SUM(SF.TotalAmount) - LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY DS.StoreID ORDER BY DT.Quarter)) 
            / LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY DS.StoreID ORDER BY DT.Quarter)) * 100, 2)
    END AS GrowthRate
FROM SALES_FACT SF
JOIN DIM_STORE DS ON SF.Store_SK = DS.Store_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
WHERE YEAR(DT.TransactionDate) = 2017
GROUP BY 
    DS.StoreID,
    DS.StoreName,
    DT.Quarter
ORDER BY DS.StoreID, DT.Quarter;

-- ======================================
-- Q13: Detailed Supplier Sales Contribution by Store and Product Name
-- ======================================
SELECT
    DS.StoreName,
    DSP.SupplierName,
    P.Product_ID,
    P.Product_Category,
    SUM(SF.Quantity) AS TotalQuantity,
    SUM(SF.TotalAmount) AS TotalSales,
    AVG(SF.TotalAmount) AS AvgSaleValue,
    COUNT(*) AS TransactionCount
FROM SALES_FACT SF
JOIN DIM_STORE DS ON SF.Store_SK = DS.Store_SK
JOIN DIM_SUPPLIER DSP ON SF.Supplier_SK = DSP.Supplier_SK
JOIN DIM_PRODUCT P ON SF.Product_SK = P.Product_SK
GROUP BY 
    DS.StoreName,
    DSP.SupplierName,
    P.Product_ID,
    P.Product_Category
ORDER BY DS.StoreName, DSP.SupplierName, P.Product_ID;

-- ======================================
-- Q14: Seasonal Analysis of Product Sales Using Dynamic Drill-Down
-- ======================================
SELECT
    P.Product_Category,
    DT.Season,
    SUM(SF.Quantity) AS TotalQuantity,
    SUM(SF.TotalAmount) AS TotalSales,
    AVG(SF.TotalAmount) AS AvgSaleValue,
    COUNT(DISTINCT DC.Customer_SK) AS UniqueCustomers,
    COUNT(*) AS TransactionCount
FROM SALES_FACT SF
JOIN DIM_PRODUCT P ON SF.Product_SK = P.Product_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
JOIN DIM_CUSTOMER DC ON SF.Customer_SK = DC.Customer_SK
GROUP BY 
    P.Product_Category,
    DT.Season
ORDER BY P.Product_Category, DT.Season;

-- ======================================
-- Q15: Store-Wise and Supplier-Wise Monthly Revenue Volatility
-- ======================================
SELECT
    DS.StoreID,
    DS.StoreName,
    DSP.SupplierID,
    DSP.SupplierName,
    YEAR(DT.TransactionDate) AS Year,
    MONTH(DT.TransactionDate) AS Month,
    SUM(SF.TotalAmount) AS MonthlyRevenue,
    LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY DS.StoreID, DSP.SupplierID ORDER BY YEAR(DT.TransactionDate), MONTH(DT.TransactionDate)) AS PreviousMonthRevenue,
    CASE 
        WHEN LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY DS.StoreID, DSP.SupplierID ORDER BY YEAR(DT.TransactionDate), MONTH(DT.TransactionDate)) IS NULL THEN 0
        ELSE ROUND(((SUM(SF.TotalAmount) - LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY DS.StoreID, DSP.SupplierID ORDER BY YEAR(DT.TransactionDate), MONTH(DT.TransactionDate))) 
            / LAG(SUM(SF.TotalAmount)) OVER (PARTITION BY DS.StoreID, DSP.SupplierID ORDER BY YEAR(DT.TransactionDate), MONTH(DT.TransactionDate))) * 100, 2)
    END AS VolatilityPercentage
FROM SALES_FACT SF
JOIN DIM_STORE DS ON SF.Store_SK = DS.Store_SK
JOIN DIM_SUPPLIER DSP ON SF.Supplier_SK = DSP.Supplier_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
GROUP BY 
    DS.StoreID,
    DS.StoreName,
    DSP.SupplierID,
    DSP.SupplierName,
    YEAR(DT.TransactionDate),
    MONTH(DT.TransactionDate)
ORDER BY DS.StoreID, DSP.SupplierID, Year, Month;

-- ======================================
-- Q16: Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)
-- ======================================
SELECT TOP 5
    P1.Product_ID AS Product1,
    P1.Product_Category AS Category1,
    P2.Product_ID AS Product2,
    P2.Product_Category AS Category2,
    COUNT(*) AS FrequencyPurchasedTogether,
    SUM(SF1.TotalAmount + SF2.TotalAmount) AS CombinedRevenue
FROM SALES_FACT SF1
JOIN SALES_FACT SF2 ON SF1.OrderID = SF2.OrderID 
    AND SF1.Product_SK < SF2.Product_SK
    AND SF1.Customer_SK = SF2.Customer_SK
JOIN DIM_PRODUCT P1 ON SF1.Product_SK = P1.Product_SK
JOIN DIM_PRODUCT P2 ON SF2.Product_SK = P2.Product_SK
GROUP BY 
    P1.Product_ID,
    P1.Product_Category,
    P2.Product_ID,
    P2.Product_Category
ORDER BY FrequencyPurchasedTogether DESC;

-- ======================================
-- Q17: Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
-- ======================================
SELECT
    DS.StoreName,
    DSP.SupplierName,
    P.Product_Category,
    YEAR(DT.TransactionDate) AS Year,
    SUM(SF.TotalAmount) AS TotalRevenue,
    COUNT(*) AS TransactionCount
FROM SALES_FACT SF
JOIN DIM_STORE DS ON SF.Store_SK = DS.Store_SK
JOIN DIM_SUPPLIER DSP ON SF.Supplier_SK = DSP.Supplier_SK
JOIN DIM_PRODUCT P ON SF.Product_SK = P.Product_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
GROUP BY ROLLUP(
    DS.StoreName,
    DSP.SupplierName,
    P.Product_Category,
    YEAR(DT.TransactionDate)
)
ORDER BY DS.StoreName, DSP.SupplierName, P.Product_Category, Year;

-- ======================================
-- Q18: Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
-- ======================================
SELECT
    P.Product_ID,
    P.Product_Category,
    P.Price,
    SUM(CASE WHEN MONTH(DT.TransactionDate) <= 6 THEN SF.TotalAmount ELSE 0 END) AS H1_Revenue,
    SUM(CASE WHEN MONTH(DT.TransactionDate) <= 6 THEN SF.Quantity ELSE 0 END) AS H1_Quantity,
    SUM(CASE WHEN MONTH(DT.TransactionDate) > 6 THEN SF.TotalAmount ELSE 0 END) AS H2_Revenue,
    SUM(CASE WHEN MONTH(DT.TransactionDate) > 6 THEN SF.Quantity ELSE 0 END) AS H2_Quantity,
    SUM(SF.TotalAmount) AS YearlyRevenue,
    SUM(SF.Quantity) AS YearlyQuantity
FROM SALES_FACT SF
JOIN DIM_PRODUCT P ON SF.Product_SK = P.Product_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
WHERE YEAR(DT.TransactionDate) = YEAR(GETDATE())
GROUP BY 
    P.Product_ID,
    P.Product_Category,
    P.Price
ORDER BY YearlyRevenue DESC;

-- ======================================
-- Q19: Identify High Revenue Spikes in Product Sales and Highlight Outliers
-- ======================================
WITH DailySales AS (
    SELECT
        P.Product_ID,
        P.Product_SK,
        P.Product_Category,
        CAST(DT.TransactionDate AS DATE) AS SaleDate,
        SUM(SF.TotalAmount) AS DailySalesAmount
    FROM SALES_FACT SF
    JOIN DIM_PRODUCT P ON SF.Product_SK = P.Product_SK
    JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
    GROUP BY P.Product_ID, P.Product_SK, P.Product_Category, CAST(DT.TransactionDate AS DATE)
),
SalesWithAvg AS (
    SELECT
        Product_ID,
        Product_SK,
        Product_Category,
        SaleDate,
        DailySalesAmount,
        AVG(DailySalesAmount) OVER (PARTITION BY Product_SK) AS AvgDailySales
    FROM DailySales
)
SELECT
    Product_ID,
    Product_Category,
    SaleDate,
    DailySalesAmount AS DailySales,
    AvgDailySales,
    CASE 
        WHEN DailySalesAmount > 2 * AvgDailySales THEN 'SPIKE'
        ELSE 'NORMAL'
    END AS SaleStatus,
    ROUND((DailySalesAmount / AvgDailySales) * 100, 2) AS PercentOfAverage
FROM SalesWithAvg
WHERE DailySalesAmount > 2 * AvgDailySales
ORDER BY DailySales DESC;

GO

-- ======================================
-- Q20: Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
-- ======================================
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT
    DS.Store_SK,
    DS.StoreName,
    YEAR(DT.TransactionDate) AS Year,
    DT.Quarter,
    SUM(SF.TotalAmount) AS TotalSales,
    SUM(SF.Quantity) AS TotalQuantity,
    COUNT(*) AS TransactionCount,
    AVG(SF.TotalAmount) AS AvgSaleValue
FROM SALES_FACT SF
JOIN DIM_STORE DS ON SF.Store_SK = DS.Store_SK
JOIN DIM_TIME DT ON SF.Date_SK = DT.Date_SK
GROUP BY 
    DS.Store_SK,
    DS.StoreName,
    YEAR(DT.TransactionDate),
    DT.Quarter;

-- Query the materialized view
SELECT 
    StoreName,
    Year,
    Quarter,
    TotalSales,
    TotalQuantity,
    TransactionCount
FROM STORE_QUARTERLY_SALES
ORDER BY StoreName, Year, Quarter;

-- ==============================================================================
-- Additional Utility Queries for DW Validation
-- ==============================================================================

-- Count records in fact table
SELECT 'SALES_FACT' AS TableName, COUNT(*) AS RecordCount FROM SALES_FACT
UNION ALL
SELECT 'DIM_CUSTOMER', COUNT(*) FROM DIM_CUSTOMER
UNION ALL
SELECT 'DIM_PRODUCT', COUNT(*) FROM DIM_PRODUCT
UNION ALL
SELECT 'DIM_STORE', COUNT(*) FROM DIM_STORE
UNION ALL
SELECT 'DIM_SUPPLIER', COUNT(*) FROM DIM_SUPPLIER
UNION ALL
SELECT 'DIM_TIME', COUNT(*) FROM DIM_TIME;

-- Revenue summary
SELECT 
    COUNT(*) AS TotalTransactions,
    SUM(TotalAmount) AS TotalRevenue,
    AVG(TotalAmount) AS AvgTransactionAmount,
    MIN(TotalAmount) AS MinAmount,
    MAX(TotalAmount) AS MaxAmount,
    SUM(Quantity) AS TotalQuantity
FROM SALES_FACT;
