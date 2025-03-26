CREATE DATABASE IF NOT EXISTS ECOMERCE_DB;

USE DATABASE ECOMERCE_DB;

CREATE SCHEMA STAGE_EXTERNAL_ETL;

USE SCHEMA STAGE_EXTERNAL_ETL;

CREATE OR REPLACE STAGE EXTERNAL_STAGE_CSV_ECOMERCE_DATA
URL = 's3://fakecompanydata/'  ----- Where the data files are gathered from
FILE_FORMAT = (TYPE = CSV      ----- Type of file_format
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'  ---- Some columns/attributes can be enclose by double quotes - " "
                SKIP_HEADER = 1);  ----- Skipping the header(1st row) so only data rows are processed



CREATE TEMPORARY TABLE ECOMERCE_ORDERS_RAW( -- Creating a table to store valid and invalid records, because a copy into can give error because of invalid dates
    Order_ID VARCHAR,
    Customer_ID VARCHAR,
    Customer_Name VARCHAR,
    Order_Date VARCHAR,
    Product VARCHAR,
    Quantity VARCHAR,
    Price VARCHAR,
    Discount VARCHAR,
    Total_Amount VARCHAR,
    Payment_Method VARCHAR,
    Shipping_Address VARCHAR,
    Status VARCHAR,
    Flag VARCHAR  -- Added Flag to the raw data records which will detirmine based on priority which table to be inserted into, this eliminates duplicates inside multiple tables
);

COPY INTO ECOMERCE_ORDERS_RAW (
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
                AND UPPER(Status) IS IN ('DELIVERED', 'SHIPPED'))
            THEN 'MISSING_SHIPPING_ADDRESS'
        WHEN TRY_TO_NUMBER(Quantity) <= 0 OR TRY_TO_DECIMAL(Price, 10, 2) <= 0
            THEN 'INVALID_QUANTITY_PRICE'
        WHEN Customer_ID IS NULL OR TRIM(Customer_ID) = ''
                OR Customer_Name IS NULL OR TRIM(Customer_Name) = ''
            THEN 'MISSING_CUSTOMER_INFO'
        ELSE 'VALID'
END;

--------------------------------------------------------------------------
--------------------------INVALID DATE RECORDS----------------------------
--------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE TD_INVALID_DATE_FORMAT ( ----- Table for invalid date records
    Order_ID INT,
    Customer_ID VARCHAR,
    Customer_Name VARCHAR,
    Order_Date VARCHAR, -- Keep invalid date records with Order_Date as Varchar format so INSERT does not fail
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
    TRY_TO_DECIMAL(Price, 10, 2) AS Price,
    TRY_TO_DECIMAL(Discount, 10, 2) AS Discount,
    TRY_TO_DECIMAL(Total_Amount, 10, 2) AS Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
FROM ECOMERCE_ORDERS_RAW
WHERE Flag = 'INVALID_DATE';



--------------------------------------------------------------------------
------------------------WITHOUT SHIPPING ADDRESS--------------------------
--------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE TD_FOR_REVIEW ( ----- Table for records without SHIPPING_ADDRESS
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
    TRY_TO_DECIMAL(Price, 10, 2) AS Price,
    TRY_TO_DECIMAL(Discount, 10, 2) AS Discount,
    TRY_TO_DECIMAL(Total_Amount, 10, 2) AS Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
FROM ECOMERCE_ORDERS_RAW
WHERE flag = 'MISSING_SHIPPING_ADDRESS';


--------------------------------------------------------------------------
----------------------------INVALID QUANTITY------------------------------
--------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE TD_INVALID_QUANTITY (
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
    TRY_TO_DECIMAL(Price, 10, 2) AS Price,
    TRY_TO_DECIMAL(Discount, 10, 2) AS Discount,
    TRY_TO_DECIMAL(Total_Amount, 10, 2) AS Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
FROM ECOMERCE_ORDERS_RAW
WHERE Flag = 'INVALID_QUANTITY_PRICE';


--------------------------------------------------------------------------
---------------------------SUSPICIOUS RECORDS-----------------------------
--------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE TD_SUSPICIOUS_RECORDS ( ----- Table for records without CUSTOMER_ID or CUSTOMER_NAME
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
    TRY_TO_NUMBER(Quantity) As Quantity,
    TRY_TO_DECIMAL(Price, 10, 2) AS Price,
    TRY_TO_DECIMAL(Discount, 10, 2) AS Discount,
    TRY_TO_DECIMAL(Total_Amount, 10, 2) AS Total_Amount,
    Payment_Method,
    Shipping_Address,
    Status
FROM ECOMERCE_ORDERS_RAW
WHERE Flag = 'MISSING_CUSTOMER_INFO';


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

            -- Cases for Discount:
            CASE
                WHEN TRY_TO_DECIMAL(Discount, 10, 2) < 0 THEN 0
                WHEN TRY_TO_DECIMAL(Discount, 10, 2) > 0.5 THEN 0.5
                ELSE TRY_TO_DECIMAL(Discount, 10, 2)
            END AS Discount,

            --Cases for Payment Method:
            CASE   
                WHEN TRIM(Payment_Method) IS NULL OR TRIM(Payment_Method) = '' THEN 'Unknown'
                ELSE Payment_Method
            END AS Payment_Method,

            Shipping_Address,
            Status

    FROM ECOMERCE_ORDERS_RAW
    WHERE Flag = 'VALID'
) AS a;
