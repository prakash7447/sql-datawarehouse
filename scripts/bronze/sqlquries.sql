--this is the bronze layer, which is used to load data

--starting of creating bronze layer
create schema bronze;

create table bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date)

--this is bulkloading instead entering one row by one row , we are loading entire data using file location and bulk inset

bulk insert bronze.crm_cust_info
from 'C:\Users\COMPUTER\Documents\ACCIOJOB_CLASSES\datasets\datasets\source_crm\cust_info.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
)

select * from bronze.crm_cust_info

--now we have to create product info table

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);



BULK INSERT bronze.crm_prd_info
from 'D:\ACCIOJOB_CLASSES\datasets\datasets\source_crm\prd_info.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
)

select* from bronze.crm_prd_info


--now lets inport sales data

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

BULK INSERT bronze.crm_sales_details
from 'D:\ACCIOJOB_CLASSES\datasets\datasets\source_crm\sales_details.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
)

select * from bronze.crm_sales_details

--creating other tables remaninig

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);

BULK INSERT bronze.erp_loc_a101
from 'D:\ACCIOJOB_CLASSES\datasets\datasets\source_erp\loc_a101.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
)

select * from bronze.erp_loc_a101

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);



BULK INSERT bronze.erp_cust_az12
from 'D:\ACCIOJOB_CLASSES\datasets\datasets\source_erp\cust_az12.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
)

select * from bronze.erp_cust_az12

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);

BULK INSERT bronze.erp_px_cat_g1v2
from 'D:\ACCIOJOB_CLASSES\datasets\datasets\source_erp\px_cat_g1v2.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
)

select * from bronze.erp_px_cat_g1v2

---This is the end of bronze layer


