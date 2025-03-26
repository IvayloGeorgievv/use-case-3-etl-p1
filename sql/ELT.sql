USE DATABASE IF NOT EXISTS ECOMERCE_DB;

-- ELT Approach to the task -> I have started doing this first and I prefered to look into both scenarious instead of focusing on only one
-- Depending on the approach there is added _ELT or _ETL at the end of the SCHEMA name to differ them
CREATE SCHEMA STAGE_EXTERNAL_ELT;

USE SCHEMA STAGE_EXTERNAL_ELT;

CREATE OR REPLACE STAGE EXTERNAL_STAGE_CSV_ECOMERCE_DATA
URL = 's3://fakecompanydata/'  ----- Where the data files are gathered from
FILE_FORMAT = (TYPE = CSV      ----- Type of file_format
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'  ---- Some columns/attributes can be enclose by double quotes - " "
                SKIP_HEADER = 1);  ----- Skipping the header(1st row) so only data rows are processed


CREATE OR REPLACE TABLE ECOMERCE_ORDERS_RAW ( -- Creating a table to store valid and invalid records, because a copy into can give error because of invalid dates
    Order_ID VARCHAR,
    Customer_ID VARCHAR,
    Customer_Name VARCHAR,
    Order_Date VARCHAR,
    Product VARCHAR,
    Quantity VARCHAR,разнообразни
    Price VARCHAR,
    Discount VARCHAR,
    Total_Amount VARCHAR,
    Payment_Method VARCHAR,
    Shipping_Address VARCHAR,
    Status VARCHAR,
    Flag VARCHAR  -- Added Flag to the raw data records which will detirmine based on priority which table to be inserted into, this eliminates duplicates inside multiple tables

);

-- Copying all records from csv into raw data table
COPY INTO ECOMERCE_ORDERS_RAW(
    Order_ID,
    Customer_ID,
    Customer_Name,
    Order_Date,
    Product,
    Quantity,
    Price,
    Discount,
    Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
)
FROM @EXTERNAL_STAGE_CSV_ECOMERCE_DATA;


-- Based on the order of priority I updated the flag in the raw data records and later that would help me with inserting them into the correct tables
UPDATE ECOMERCE_ORDERS_RAW
SET Flag = CASE
        WHEN TRY_TO_DATE(Order_Date, 'YYYY-MM-DD') IS NULL
            THEN 'INVALID_DATE'
        WHEN ((Shipping_Address IS NULL OR TRIM(Shipping_Address) = '')
                AND UPPER(Status) IN ('DELIVERED', 'SHIPPED'))
            THEN 'MISSING_SHIPPING_ADDRESS'
        WHEN TRY_TO_NUMBER(Quantity) <= 0 OR TRY_TO_DECIMAL(Price, 10, 2) <= 0
            THEN 'INVALID_QUANTITY_PRICE'
        WHEN Customer_ID IS NULL OR TRIM(Customer_ID) = ''
                OR Customer_Name IS NULL OR TRIM(Customer_Name) = ''
            THEN 'MISSING_CUSTOMER_INFO'
        ELSE 'VALID'
END; 


--Count of records with the grouped by Flags
SELECT Flag, COUNT(*) AS Record_Count
FROM ECOMERCE_ORDERS_RAW
GROUP BY Flag;


--------------------------------------------------------------------------
--------------------------INVALID DATE RECORDS----------------------------
--------------------------------------------------------------------------

CREATE OR REPLACE TABLE TD_INVALID_DATE_FORMAT ( ----- Table for invalid date records
    Order_ID INT,
    Customer_ID VARCHAR,
    Customer_Name VARCHAR,
    Order_Date VARCHAR, --- Keep invalid date records with Order_Date in VARCHAR format so the copy into can go trough
    Product VARCHAR,
    Quantity INT,
    Price FLOAT,
    Discount FLOAT,
    Total_Amount FLOAT,
    Payment_Method VARCHAR,
    Shipping_Address VARCHAR,
    Status VARCHAR
);

INSERT INTO TD_INVALID_DATE_FORMAT (
    Order_ID,
    Customer_ID,
    Customer_Name,
    Order_Date,
    Product,
    Quantity,
    Price,
    Discount,
    Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
)
SELECT DISTINCT
    TRY_TO_NUMBER(Order_ID) AS Order_ID,
    Customer_ID,
    Customer_Name,
    Order_Date,
    Product,
    TRY_TO_NUMBER(Quantity) AS Quantity,
    TRY_TO_DECIMAL(Price, 10,2) AS Price,
    TRY_TO_DECIMAL(Discount, 10, 2) AS Discount,
    TRY_TO_DECIMAL(Total_Amount, 10, 2) AS Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
FROM ECOMERCE_ORDERS_RAW
WHERE Flag = 'INVALID_DATE';


-- Preview of a few records with Invalid Date
SELECT *
FROM TD_INVALID_DATE_FORMAT
LIMIT 10;


--------------------------------------------------------------------------
------------------------WITHOUT SHIPPING ADDRESS--------------------------
--------------------------------------------------------------------------


CREATE OR REPLACE TABLE TD_FOR_REVIEW ( ----- Table for records without SHIPPING_ADDRESS
    Order_ID INT,
    Customer_ID VARCHAR,
    Customer_Name VARCHAR,
    Order_Date DATE, 
    Product VARCHAR,
    Quantity INT,
    Price FLOAT,
    Discount FLOAT,
    Total_Amount FLOAT,
    Payment_Method VARCHAR,
    Shipping_Address VARCHAR,
    Status VARCHAR
);

INSERT INTO TD_FOR_REVIEW (
    Order_ID,
    Customer_ID,
    Customer_Name,
    Order_Date,
    Product,
    Quantity,
    Price,
    Discount,
    Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
)
SELECT DISTINCT
    TRY_TO_NUMBER(Order_ID) AS Order_ID,
    Customer_ID,
    Customer_Name,
    TRY_TO_DATE(Order_Date, 'YYYY-MM-DD') AS Order_Date,
    Product,
    TRY_TO_NUMBER(Quantity) AS Quantity,
    TRY_TO_DECIMAL(Price, 10,2) AS Price,
    TRY_TO_DECIMAL(Discount, 10, 2) AS Discount,
    TRY_TO_DECIMAL(Total_Amount, 10, 2) AS Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
FROM ECOMERCE_ORDERS_RAW
WHERE Flag = 'MISSING_SHIPPING_ADDRESS';


-- Preview of a few records without Shipping Address
SELECT *
FROM TD_FOR_REVIEW
LIMIT 10;


--------------------------------------------------------------------------
----------------------------INVALID QUANTITY------------------------------
--------------------------------------------------------------------------

CREATE TABLE TD_INVALID_QUANTITY (
    Order_ID INT,
    Customer_ID VARCHAR,
    Customer_Name VARCHAR,
    Order_Date DATE, 
    Product VARCHAR,
    Quantity INT,
    Price FLOAT,
    Discount FLOAT,
    Total_Amount FLOAT,
    Payment_Method VARCHAR,
    Shipping_Address VARCHAR,
    Status VARCHAR
);

INSERT INTO TD_INVALID_QUANTITY (
    Order_ID,
    Customer_ID,
    Customer_Name,
    Order_Date,
    Product,
    Quantity,
    Price,
    Discount,
    Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
)
SELECT DISTINCT
    TRY_TO_NUMBER(Order_ID) AS Order_ID,
    Customer_ID,
    Customer_Name,
    TRY_TO_DATE(Order_Date, 'YYYY-MM-DD') AS Order_Date,
    Product,
    TRY_TO_NUMBER(Quantity) AS Quantity,
    TRY_TO_DECIMAL(Price, 10,2) AS Price,
    TRY_TO_DECIMAL(Discount, 10, 2) AS Discount,
    TRY_TO_DECIMAL(Total_Amount, 10, 2) AS Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
FROM ECOMERCE_ORDERS_RAW
WHERE Flag = 'INVALID_QUANTITY_PRICE';


-- Preview of a few records with Invalid Quantity or Price
SELECT *
FROM TD_INVALID_QUANTITY
LIMIT 10;


--------------------------------------------------------------------------
---------------------------SUSPICIOUS RECORDS-----------------------------
--------------------------------------------------------------------------

CREATE OR REPLACE TABLE TD_SUSPICIOUS_RECORDS ( ----- Table for records without CUSTOMER_ID or CUSTOMER_NAME
    Order_ID INT,
    Customer_ID VARCHAR,
    Customer_Name VARCHAR,
    Order_Date DATE, 
    Product VARCHAR,
    Quantity INT,
    Price FLOAT,
    Discount FLOAT,
    Total_Amount FLOAT,
    Payment_Method VARCHAR,
    Shipping_Address VARCHAR,
    Status VARCHAR
);


INSERT INTO TD_SUSPICIOUS_RECORDS (
    Order_ID,
    Customer_ID,
    Customer_Name,
    Order_Date,
    Product,
    Quantity,
    Price,
    Discount,
    Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
)
SELECT DISTINCT
    TRY_TO_NUMBER(Order_ID) AS Order_ID,
    Customer_ID,
    Customer_Name,
    TRY_TO_DATE(Order_Date, 'YYYY-MM-DD') AS Order_Date,
    Product,
    TRY_TO_NUMBER(Quantity) AS Quantity,
    TRY_TO_DECIMAL(Price, 10,2) AS Price,
    TRY_TO_DECIMAL(Discount, 10, 2) AS Discount,
    TRY_TO_DECIMAL(Total_Amount, 10, 2) AS Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
FROM ECOMERCE_ORDERS_RAW
WHERE Flag = 'MISSING_CUSTOMER_INFO';


-- Preview of a few records without Customer ID or Customer Name
SELECT *
FROM TD_SUSPICIOUS_RECORDS
LIMIT 10;


--------------------------------------------------------------------------
-------------------------------VALID DATA---------------------------------
--------------------------------------------------------------------------


CREATE TABLE TD_CLEAN_RECORDS (
    Order_ID INT,
    Customer_ID VARCHAR,
    Customer_Name VARCHAR,
    Order_Date DATE, 
    Product VARCHAR,
    Quantity INT,
    Price FLOAT,
    Discount FLOAT,
    Total_Amount FLOAT,
    Payment_Method VARCHAR,
    Shipping_Address VARCHAR,
    Status VARCHAR
);


INSERT INTO TD_CLEAN_RECORDS
SELECT Order_ID,
        Customer_ID,
        Customer_Name,
        Order_Date,
        Product,
        Quantity,
        Price,
        Discount,
        -- Calculate Actual Total Price based on fixed discount, quantity and price
        (Quantity * Price * (1 - Discount)) AS Total_Amount,
        Payment_Method,
        Shipping_Address,
        Status
FROM (
     SELECT DISTINCT
            TRY_TO_NUMBER(Order_ID) AS Order_ID,
            Customer_ID,
            Customer_Name,
            Product,
            TRY_TO_NUMBER(Quantity) AS Quantity,
            TRY_TO_DECIMAL(Price, 10, 2) AS Price,
            TRY_TO_DATE(Order_Date, 'YYYY-MM-DD') AS Order_Date,

             --Cases for Discount fix:
            CASE
                WHEN TRY_TO_DECIMAL(Discount, 10, 2) < 0 THEN 0
                WHEN TRY_TO_DECIMAL(Discount, 10, 2) > 0.5 THEN 0.5
                ELSE TRY_TO_DECIMAL(Discount, 10, 2)
            END AS Discount,

            -- Cases for Payment Method:
            CASE 
                WHEN TRIM(Payment_Method) IS NULL OR TRIM(Payment_Method) = '' THEN 'Unknown'
                ELSE Payment_Method
            END AS Payment_Method,

            Shipping_Address,
            Status

    FROM ECOMERCE_ORDERS_RAW
    WHERE Flag = 'VALID'
) AS a;



-- Preview of a few VALID records
SELECT *
FROM TD_CLEAN_RECORDS
LIMIT 10;


-- Count of unique records in the raw table:
SELECT COUNT(*) AS Raw_Record_Count
FROM (
    SELECT DISTINCT *
    FROM ECOMERCE_ORDERS_RAW
) AS t;


-- Count of records in different tables and in all tables collected:
SELECT 'TD_INVALID_DATE_FORMAT' AS Table_Name, COUNT(*) AS Record_Count 
FROM TD_INVALID_DATE_FORMAT
UNION ALL
SELECT 'TD_FOR_REVIEW' AS Table_Name, COUNT(*) 
FROM TD_FOR_REVIEW
UNION ALL
SELECT 'TD_SUSPICIOUS_RECORDS' AS Table_Name, COUNT(*) 
FROM TD_SUSPICIOUS_RECORDS
UNION ALL
SELECT 'TD_INVALID_QUANTITY' AS Table_Name, COUNT(*) 
FROM TD_INVALID_QUANTITY
UNION ALL
SELECT 'TD_CLEAN_RECORDS' AS Table_Name, COUNT(*) 
FROM TD_CLEAN_RECORDS
UNION ALL
SELECT 'TOTAL_RECORDS' AS Table_Name, 
       ((SELECT COUNT(*) FROM TD_INVALID_DATE_FORMAT) +
        (SELECT COUNT(*) FROM TD_FOR_REVIEW) +
        (SELECT COUNT(*) FROM TD_SUSPICIOUS_RECORDS) +
        (SELECT COUNT(*) FROM TD_INVALID_QUANTITY) +
        (SELECT COUNT(*) FROM TD_CLEAN_RECORDS)
       ) AS Record_Count;
