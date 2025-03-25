USE DATABASE IF NOT EXISTS ECOMERCE_DB;

---I started doing an ELT method of finishing the task first so I renamed the SCHEMA to --//--_ELT and I will create one with ETL method as well
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
    Quantity VARCHAR,
    Price VARCHAR,
    Discount VARCHAR,
    Total_Amount VARCHAR,
    Payment_Method VARCHAR,
    Shipping_Address VARCHAR,
    Status VARCHAR
);

-- Copying all records from csv into raw data table
COPY INTO ECOMERCE_ORDERS_RAW
FROM @EXTERNAL_STAGE_CSV_ECOMERCE_DATA;



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
WHERE TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD') IS NULL; -- With that only the records which dates fail to convert to date are inserted inside TD_INVALID_DATE_FORMAT


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
WHERE SHIPPING_ADDRESS IS NULL AND STATUS LIKE 'Delivered';



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
WHERE CUSTOMER_ID IS NULL OR CUSTOMER_NAME IS NULL;


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
WHERE QUANTITY <= 0 OR PRICE <= 0;





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

            -- Validating Status:
            CASE 
                WHEN (Shipping_Address IS NULL OR TRIM(Shipping_Address) = '')
                    AND (UPPER(Status) = 'DELIVERED' OR UPPER(Status) = 'SHIPPED')  -- Order CANNOT be shipped or delivered if the Shipping_Adress is NULL
                THEN 'Pending'
                ELSE Status    
            END AS Status
    FROM ECOMERCE_ORDERS_RAW
) AS a
WHERE Order_Date IS NOT NULL
    AND Quantity > 0
    AND Customer_ID IS NOT NULL AND TRIM(Customer_ID) != ''
    AND Customer_Name IS NOT NULL AND TRIM(Customer_Name) != ''; 
