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
