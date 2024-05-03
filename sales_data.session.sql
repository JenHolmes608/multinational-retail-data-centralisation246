/*
SQL file for data type adjustments and foreign key constraints setup.

This SQL file contains statements to adjust data types in various tables and set up foreign key constraints between the 'orders_table' and related dimension tables.

Author: [Your Name]
Date: [Date of Creation]
*/

-- Adjusting data types in 'orders_table'
SELECT 
    MAX(LENGTH(card_number)) AS max_length,
    MAX(LENGTH(store_code)) AS max_length1,
    MAX(LENGTH(product_code)) AS max_length2
FROM orders_table;

-- Alter orders_table columns to appropriate data types and lengths
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT;

-- Update data types and column lengths in dim_users
-- Set max_length3 variable for country_code
SELECT MAX(LENGTH(country_code)) AS max_length3
FROM dim_users;

-- Alter dim_users columns to appropriate data types and lengths
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(3),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN join_date TYPE DATE;

-- Update data types and column lengths in dim_store_details
-- Set max_length4 and max_length5 variables for store_code and country_code
SELECT 
    MAX(LENGTH(store_code)) AS max_length4,
    MAX(LENGTH(country_code)) AS max_length5
FROM dim_store_details;

-- Alter dim_store_details columns to appropriate data types and lengths
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN staff_numbers TYPE SMALLINT USING NULLIF(staff_numbers, '')::SMALLINT;
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255);

-- Update data types and column values in dim_products
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

-- Add column to group categorrise different weights
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

-- Convert various units to kg
UPDATE dim_products
SET weight = CASE 
               WHEN weight ~ '^\d+(\.\d+)?\s*kg$' THEN REPLACE(weight, 'kg', '')::FLOAT
               WHEN weight ~ '^\d+(\.\d+)?\s*g$' THEN REPLACE(REPLACE(weight, 'g', ''), 'x', '*')::FLOAT / 1000
               ELSE NULL
            END,
    weight_class = CASE 
                     WHEN weight ~ '^\d+(\.\d+)?\s*kg$' THEN
                       CASE
                         WHEN REPLACE(weight, 'kg', '')::FLOAT < 2 THEN 'Light'
                         WHEN REPLACE(weight, 'kg', '')::FLOAT >= 2 AND REPLACE(weight, 'kg', '')::FLOAT < 40 THEN 'Mid_Sized'
                         WHEN REPLACE(weight, 'kg', '')::FLOAT >= 40 AND REPLACE(weight, 'kg', '')::FLOAT < 140 THEN 'Heavy'
                         WHEN REPLACE(weight, 'kg', '')::FLOAT >= 140 THEN 'Truck_Required'
                         ELSE 'Unknown'
                       END
                     WHEN weight ~ '^\d+(\.\d+)?\s*g$' THEN
                       CASE
                         WHEN REPLACE(REPLACE(weight, 'g', ''), 'x', '*')::FLOAT / 1000 < 2 THEN 'Light'
                         WHEN REPLACE(REPLACE(weight, 'g', ''), 'x', '*')::FLOAT / 1000 >= 2 AND REPLACE(REPLACE(weight, 'g', ''), 'x', '*')::FLOAT / 1000 < 40 THEN 'Mid_Sized'
                         WHEN REPLACE(REPLACE(weight, 'g', ''), 'x', '*')::FLOAT / 1000 >= 40 AND REPLACE(REPLACE(weight, 'g', ''), 'x', '*')::FLOAT / 1000 < 140 THEN 'Heavy'
                         WHEN REPLACE(REPLACE(weight, 'g', ''), 'x', '*')::FLOAT / 1000 >= 140 THEN 'Truck_Required'
                         ELSE 'Unknown'
                       END
                     ELSE NULL
                   END;

--ALter name of dim_products column
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- Set max_length6 and max_length7 variables for EAN and product_code
SELECT 
    MAX(LENGTH("EAN")) AS max_length6,
    MAX(LENGTH(product_code)) AS max_length7
FROM dim_products;

-- Alter dim_products columns to appropriate data types and lengths
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE DOUBLE PRECISION USING product_price::DOUBLE PRECISION,
ALTER COLUMN weight TYPE DOUBLE PRECISION USING weight::DOUBLE PRECISION,
ALTER COLUMN "EAN" TYPE VARCHAR(17),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid TYPE UUID USING UUID::UUID,
ALTER COLUMN still_available TYPE BOOLEAN USING CASE WHEN still_available = 'removed' THEN FALSE ELSE TRUE END,
ALTER COLUMN weight_class TYPE VARCHAR(14);

-- Update data types and column lengths in dim_dates_times
-- Set max_length8 variable for time_period
SELECT MAX(LENGTH(time_period)) AS max_length8
FROM dim_dates_times;

-- Alter dim_dates_times columns to appropriate data types and lengths
ALTER TABLE dim_dates_times
ALTER COLUMN month TYPE VARCHAR(9),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- Update data types and column lengths in dim_card_details
-- Set max_length9 and max_length10 variables for card_number and expiry_date
SELECT 
    MAX(LENGTH(card_number)) AS max_length9,
    MAX(LENGTH(expiry_date)) AS max_length10
FROM dim_card_details;

-- Alter dim_card_details columns to appropriate data types and lengths
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE;

DELETE FROM dim_card_details
WHERE card_number IS NULL;

-- ===========================================================================
-- SECTION 2: Adding Primary Keys
-- ===========================================================================

-- Add primary keys to dimension tables
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_dates_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

-- ===========================================================================
-- SECTION 3: Creating Foreign Key Constraints
-- ===========================================================================

-- Create foreign key constraints in the orders_table to reference dimension tables
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_users
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_details
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_dates_times
FOREIGN KEY (date_uuid)
REFERENCES dim_dates_times (date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_card_details
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);