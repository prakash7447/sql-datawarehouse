insert into silver.crm_cust_info
select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case
 when UPPER(trim(cst_marital_status)) = 'S' THEN 'Single'
 when UPPER(trim(cst_marital_status)) = 'M' THEN 'Married'
 else 'n/a'
end as cst_marital_status,
case
 when UPPER(trim(cst_gndr)) = 'F' THEN 'Female'
 when UPPER(trim(cst_gndr)) = 'M' THEN 'Male'
 else 'n/a'
end as cst_gndr,
cst_create_date
from
(
select *,
ROW_NUMBER() over(partition by cst_id
order by cst_create_date desc
) as cust_rank_by_date
from bronze.crm_cust_info
) as t
where (cust_rank_by_date = 1 and cst_id is not null);


select * from silver.crm_cust_info

--inserting into crm_prd_info
insert into silver.crm_prd_info
(prd_id,
 cat_id,
 prd_key,
 prd_nm,          
 prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt )
 (
select prd_id,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,len(prd_key)-1) as prd_key,
prd_nm,
isnull(prd_cost,0) as prd_cost,
case 
when upper(trim(prd_line)) ='M' then 'Mountain'
when upper(trim(prd_line)) ='R' then 'Road'
when upper(trim(prd_line)) ='S' then 'Other sales'
when upper(trim(prd_line)) ='T' then 'Touring'
else 'n/a'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date ) as prd_end_dt
from bronze.crm_prd_info);


--inserting into silver.crm_sales_details

INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;


--inserting into silver.erp_cust_az12

insert into silver.erp_cust_az12
select 
case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
else cid 
end as cid_,
case 
when bdate>getdate() then null
else bdate
end as bdate,
case
when upper(trim(gen)) in ('M','MALE') Then 'Male'
when upper(trim(gen)) in ('F','FEMALE') Then 'Female'
else 'n/a'
end as gen
from bronze.erp_cust_az12



-- inserting into the silver.erp_loc

insert into silver.erp_loc_a101
select 
replace(cid,'-','')as cid,
case
when trim(cntry) ='DE' then 'Germany'
when trim(cntry) in ('US','USA') then 'United States'
when trim(cntry) ='' or cntry is null then 'n/a'
else trim(cntry)
end as cntry
from bronze.erp_loc_a101


--inserting into silver schema table

insert into silver.erp_px_cat_g1v2
select * from bronze.erp_px_cat_g1v2
