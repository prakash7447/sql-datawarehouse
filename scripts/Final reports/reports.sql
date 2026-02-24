-- Product report

CREATE VIEW gold.report_products as
with base_query as (
SELECT
p.product_key,
p.product_name,
p.category,
p.subcategory,
p.cost,
f.order_date,
f.order_number,
f.customer_key,
f.sales_amount,
f.quantity
from gold.dim_products p
left join gold.fact_sales f 
on p.product_key = f.product_key
where order_date is not null
), cte2 as (
select
product_key,
product_name,
category,
subcategory,
cost,
DATEDIFF(month,min(order_date),max(order_date)) as lifespan,
max(order_date) as last_sale_date,
Count(distinct order_number) as total_orders,
Count(distinct customer_key) as total_Customers,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
avg(sales_amount / quantity) as avg_selling_price
from base_query
group by
product_key,
product_name,
category,
subcategory,
cost
)
select 
product_key,
product_name,
category,
subcategory,
cost,
last_sale_date,
datediff(month,last_sale_date,getdate()) as recency_in_months,
case
 when total_sales >= 50000 then 'High-Performer'
 when total_sales >= 100 then 'Mid-Range'
 else 'Low Performer'
end as product_segment,
lifespan,
total_orders,
total_sales,
total_quantity,
total_customers,
avg_selling_price,
case
 when total_orders = 0 then 0
 else total_sales / total_orders
end as avg_order_revenue,
case
 when lifespan = 0 then total_sales
 else total_sales/lifespan
end as avg_monthly_revenue
from cte2;




-- Customer report



CREATE view gold.report_customers as
with base_query as (
SELECT 
c.customer_key,
c.customer_number,
CONCAT(c.first_name,' ',c.last_name) as customer_name,
datediff(year,c.birthdate,getdate()) as age,
f.order_date,
f.order_number,
f.sales_amount,
f.quantity
from gold.dim_customers c
left join gold.fact_sales f
on c.customer_key = f.customer_key
where f.order_date is not null
),customer_agg as (
select
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
datediff(month,min(order_date), max(order_date)) as lifespan,
max(order_date) as last_order_date
from base_query
group by
customer_key,
customer_number,
customer_name,
age
)
select
customer_key,
customer_number,
customer_name,
age,
CASE
 when age < 20 then 'Under 20'
 when age between 20 and 29 then '20-29'
 when age between 30 and 39 then '30-39'
 when age between 40 and 49 then '40-49'
 else '50 and above'
end as age_group,
CASE
 when lifespan >= 12 and total_sales > 5000 then 'VIP'
 when lifespan >= 12 and total_sales <= 5000 then 'Regular'
 else 'New'
end as customer_segment,
last_order_date,
DATEDIFF(month,last_order_date,getdate()) as recency,
total_orders,
total_sales,
total_quantity,
lifespan,
case
 when total_sales = 0 THEN 0
 else total_sales / total_orders
end as avg_order_value,
case
 when lifespan = 0 then total_sales
 else total_sales / lifespan
end as avg_monthly_spend
from customer_agg

