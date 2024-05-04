-- Query 1
-- Description: Count the total number of stores in each country and sort them in descending order.
-- Returns: Country code and the total number of stores.
SELECT country_code, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- Query 2
-- Description: Count the total number of stores in each locality and sort them in descending order.
-- Returns: Locality and the total number of stores.
SELECT locality, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC;

-- Query 3
-- Description: Calculate the total sales for each month.
-- Returns: Total sales and the month.
SELECT 
    SUM(dp.product_price * o.product_quantity) AS total_sales,
    EXTRACT(MONTH FROM CAST(TO_TIMESTAMP(o.date_uuid::text, 'YYYY-MM-DD') AS TIMESTAMP)) AS month
FROM 
    orders_table o
JOIN 
    dim_products dp ON o.product_code = dp.product_code
GROUP BY 
    EXTRACT(MONTH FROM CAST(TO_TIMESTAMP(o.date_uuid::text, 'YYYY-MM-DD') AS TIMESTAMP))
ORDER BY 
    total_sales DESC;

-- Query 4
-- Description: Calculate the total number of sales, total product quantity, and classify them by location.
-- Returns: Number of sales, total product quantity, and location (Web or Offline).
SELECT 
    COUNT(*) AS numbers_of_sales,
    SUM(o.product_quantity) AS product_quantity_count,
    CASE 
        WHEN dsd.store_type = 'Web' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM 
    orders_table o
JOIN 
    dim_store_details dsd ON o.store_code = dsd.store_code
GROUP BY 
    CASE 
        WHEN dsd.store_type = 'Web' THEN 'Web'
        ELSE 'Offline'
    END;

-- Query 5
-- Description: Calculate the total sales for each store type and the percentage of each type in total revenue.
-- Returns: Store type, total sales, and percentage of total revenue.
SELECT
    sd.store_type,
    SUM(o.product_quantity * p.product_price) AS total_sales,
    (SUM(o.product_quantity * p.product_price) / total_revenue.total) * 100 AS percentage_total
FROM
    dim_store_details sd
JOIN
    orders_table o ON sd.store_code = o.store_code
JOIN
    dim_products p ON o.product_code = p.product_code
CROSS JOIN
    (SELECT SUM(product_quantity * product_price) AS total FROM orders_table) AS total_revenue
GROUP BY
    sd.store_type, total_revenue.total;

-- Query 6
-- Description: Calculate the total sales for each year and month.
-- Returns: Total sales, year, and month.
SELECT 
    SUM(dp.product_price * o.product_quantity) AS total_sales,
    EXTRACT(YEAR FROM CAST(TO_TIMESTAMP(o.date_uuid::text, 'YYYY-MM-DD') AS TIMESTAMP)) AS year,
    EXTRACT(MONTH FROM CAST(TO_TIMESTAMP(o.date_uuid::text, 'YYYY-MM-DD') AS TIMESTAMP)) AS month
FROM 
    orders_table o
JOIN 
    dim_products dp ON o.product_code = dp.product_code
GROUP BY 
    EXTRACT(YEAR FROM CAST(TO_TIMESTAMP(o.date_uuid::text, 'YYYY-MM-DD') AS TIMESTAMP)),
    EXTRACT(MONTH FROM CAST(TO_TIMESTAMP(o.date_uuid::text, 'YYYY-MM-DD') AS TIMESTAMP))
ORDER BY 
    total_sales DESC;

-- Query 7
-- Description: Calculate the total staff numbers in each country.
-- Returns: Total staff numbers and country code
SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY
    country_code;

-- Query 8
-- Description: Calculate the total sales for each store type in Germany.
-- Returns: Total sales, store type, and country code for Germany.
SELECT
    SUM(o.product_quantity * p.product_price) AS total_sales,
    sd.store_type,
    sd.country_code
FROM
    orders_table o
JOIN
    dim_products p ON o.product_code = p.product_code
JOIN
    dim_store_details sd ON o.store_code = sd.store_code
WHERE
    sd.country_code = 'DE'
GROUP BY
    sd.store_type, sd.country_code
ORDER BY
    total_sales DESC;

-- Query 9
-- Description: Calculate the average time taken between each sale grouped by year.
-- Returns: Year and the average time taken between each sale.
SELECT
    EXTRACT(YEAR FROM date_uuid) AS year,
    AVG(EXTRACT(EPOCH FROM (lead(timestamp) OVER (ORDER BY timestamp)) - timestamp))::interval AS actual_time_taken
FROM
    dim_dates_times
GROUP BY
    year
ORDER BY
    year;