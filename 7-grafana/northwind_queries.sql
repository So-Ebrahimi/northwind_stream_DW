-- ============================================
-- Northwind Data Warehouse Queries for ClickHouse
-- ============================================

-- 1. Top 10 Products by Sales
SELECT 
    dp.ProductName,
    dp.CategoryName,
    SUM(fo.TotalAmount) AS TotalSales,
    SUM(fo.Quantity) AS TotalQuantitySold
FROM FactOrders fo
INNER JOIN DimProducts dp ON fo.ProductKey = dp.ProductKey
GROUP BY dp.ProductName, dp.CategoryName
ORDER BY TotalSales DESC
LIMIT 10;

-- 2. Sales & Orders per Country
SELECT 
    dg.country,
    COUNT(DISTINCT fo.OrderAlternateKey) AS NumberOfOrders,
    SUM(fo.TotalAmount) AS TotalSales,
    SUM(fo.Quantity) AS TotalQuantitySold,
    AVG(fo.TotalAmount) AS AverageOrderValue
FROM FactOrders fo
INNER JOIN DimCustomer dc ON fo.CustomerKey = dc.CustomerKey
INNER JOIN DimGeography dg ON dc.GeographyKey = dg.GeographyKey
GROUP BY dg.country
ORDER BY TotalSales DESC;

-- 3. Top 5 Companies by Sales
SELECT 
    dc.CompanyName,
    dc.ContactName,
    SUM(fo.TotalAmount) AS TotalSales,
    COUNT(DISTINCT fo.OrderAlternateKey) AS NumberOfOrders,
    SUM(fo.Quantity) AS TotalQuantitySold
FROM FactOrders fo
INNER JOIN DimCustomer dc ON fo.CustomerKey = dc.CustomerKey
GROUP BY dc.CompanyName, dc.ContactName
ORDER BY TotalSales DESC
LIMIT 5;

-- 4. Units in Stock per Products
SELECT 
    ProductName,
    CategoryName,
    units_in_stock,
    units_on_order,
    reorder_level,
    CASE 
        WHEN units_in_stock <= reorder_level THEN 'Low Stock'
        WHEN units_in_stock <= (reorder_level * 2) THEN 'Medium Stock'
        ELSE 'Good Stock'
    END AS StockStatus
FROM DimProducts
ORDER BY units_in_stock ASC;

-- 5. Sales per Months & Quarter
-- Option 1: If OrderDate is a Unix timestamp (seconds since epoch)
SELECT 
    toYear(toDateTime(fo.OrderDate)) AS Year,
    toQuarter(toDateTime(fo.OrderDate)) AS Quarter,
    toMonth(toDateTime(fo.OrderDate)) AS Month,
    formatDateTime(toDateTime(fo.OrderDate), '%B') AS MonthName,
    COUNT(DISTINCT fo.OrderAlternateKey) AS NumberOfOrders,
    SUM(fo.TotalAmount) AS TotalSales,
    SUM(fo.Quantity) AS TotalQuantitySold,
    AVG(fo.TotalAmount) AS AverageOrderValue
FROM FactOrders fo
GROUP BY Year, Quarter, Month, MonthName
ORDER BY Year DESC, Quarter DESC, Month DESC;

-- Option 2: If OrderDate is a DateKey (YYYYMMDD format) - uncomment if needed
-- SELECT 
--     dd.Year,
--     dd.Quarter,
--     dd.Month,
--     dd.MonthName,
--     COUNT(DISTINCT fo.OrderAlternateKey) AS NumberOfOrders,
--     SUM(fo.TotalAmount) AS TotalSales,
--     SUM(fo.Quantity) AS TotalQuantitySold,
--     AVG(fo.TotalAmount) AS AverageOrderValue
-- FROM FactOrders fo
-- INNER JOIN DimDate dd ON CAST(fo.OrderDate AS UInt32) = dd.DateKey
-- GROUP BY dd.Year, dd.Quarter, dd.Month, dd.MonthName
-- ORDER BY dd.Year DESC, dd.Quarter DESC, dd.Month DESC;

-- 6. Orders per Shipper Company
SELECT 
    ds.company_name AS ShipperCompany,
    ds.phone AS ShipperPhone,
    COUNT(DISTINCT fo.OrderAlternateKey) AS NumberOfOrders,
    SUM(fo.TotalAmount) AS TotalSales,
    SUM(fo.Freight) AS TotalFreight,
    AVG(fo.Freight) AS AverageFreight
FROM FactOrders fo
INNER JOIN DimShippers ds ON fo.ShipperKey = ds.ShipperKey
GROUP BY ds.company_name, ds.phone
ORDER BY NumberOfOrders DESC;

-- 7. Total Number of Orders
SELECT 
    COUNT(DISTINCT OrderAlternateKey) AS TotalNumberOfOrders,
    COUNT(*) AS TotalOrderLines
FROM FactOrders;

-- 8. Total Revenue
SELECT 
    SUM(TotalAmount) AS TotalRevenue,
    SUM(Freight) AS TotalFreight,
    SUM(TotalAmount) + SUM(Freight) AS TotalRevenueWithFreight,
    AVG(TotalAmount) AS AverageOrderValue,
    MIN(TotalAmount) AS MinOrderValue,
    MAX(TotalAmount) AS MaxOrderValue
FROM FactOrders;

-- 9. Total Number of Companies (Customers)
SELECT 
    COUNT(DISTINCT CustomerKey) AS TotalNumberOfCompanies,
    COUNT(DISTINCT CompanyName) AS UniqueCompanyNames
FROM DimCustomer;

-- 10. Total Number of Products
SELECT 
    COUNT(DISTINCT ProductKey) AS TotalNumberOfProducts,
    COUNT(DISTINCT CategoryName) AS NumberOfCategories,
    SUM(CASE WHEN discontinued = 1 THEN 1 ELSE 0 END) AS DiscontinuedProducts,
    SUM(CASE WHEN discontinued = 0 THEN 1 ELSE 0 END) AS ActiveProducts
FROM DimProducts;

