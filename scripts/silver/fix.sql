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
