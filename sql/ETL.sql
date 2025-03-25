CREATE DATABASE IF NOT EXISTS ECOMERCE_DB;

USE DATABASE ECOMERCE_DB;

CREATE SCHEMA STAGE_EXTERNAL_ETL;

USE SCHEMA STAGE_EXTERNAL_ETL;

CREATE OR REPLACE STAGE EXTERNAL_STAGE_CSV_ECOMERCE_DATA
URL = 's3://fakecompanydata/'  ----- Where the data files are gathered from
FILE_FORMAT = (TYPE = CSV      ----- Type of file_format
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'  ---- Some columns/attributes can be enclose by double quotes - " "
                SKIP_HEADER = 1);  ----- Skipping the header(1st row) so only data rows are processed



CREATE TEMPORARY TABLE ECOMERCE_ORDERS_RAW(
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
    Status VARCHAR
);

COPY INTO ECOMERCE_ORDERS_RAW
FROM @EXTERNAL_STAGE_CSV_ECOMERCE_DATA;


--------------------------------------------------------------------------
--------------------------INVALID DATE RECORDS----------------------------
--------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE TD_INVALID_DATE_FORMAT (
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
WHERE TRY_TO_DATE(Order_Date, 'YYYY-MM-DD') IS NULL; -- That way only records which have invalid Date go inside the table



--------------------------------------------------------------------------
------------------------WITHOUT SHIPPING ADDRESS--------------------------
--------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE TD_FOR_REVIEW (
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
WHERE Shipping_Address IS NULL AND Status LIKE 'Delivered';



--------------------------------------------------------------------------
---------------------------SUSPICIOUS RECORDS-----------------------------
--------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE TD_SUSPICIOUS_RECORDS (
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
WHERE Customer_ID IS NULL OR Customer_Name IS NULL;



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
WHERE Quantity <= 0 OR Price <= 0;



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

            --Validate Status:
            CASE 
                WHEN (Shipping_Address IS NULL OR TRIM(Shipping_Address) = '')
                        AND (UPPER(Status) = 'DELIVERED' OR UPPER(Status) = 'SHIPPED') -- Cannot be Delivered or Shipped if there is not a valid Shipping Address
                        THEN 'Pending'
                ELSE Status
            END AS Status
    FROM ECOMERCE_ORDERS_RAW
) AS a
WHERE Order_Date IS NOT NULL
    AND Quantity > 0
    AND Customer_ID IS NOT NULL AND TRIM(Customer_ID) != ''
    AND Customer_Name IS NOT NULL AND TRIM(Customer_Name) != '';
