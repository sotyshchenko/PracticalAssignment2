use opt_db;


select * from opt_clients oc;
select * from opt_orders oo;
select * from opt_products op;


drop index idx_opt_products_price
    on opt_products;

drop index idx_opt_orders_order_date
    on opt_orders;
   
drop index idx_opt_product_category
	on opt_products;

drop index idx_opt_client_status
	on opt_clients;

   
-- Non-optimized
   
explain analyze 
select oc.id as client_id, oo.order_id, oo.order_date, oo.product_id, op.product_name, op.product_price
from opt_clients as oc
join opt_orders as oo on oc.id = oo.client_id
join opt_products as op on oo.product_id = op.product_id
where oo.order_date > '2024-06-01'
and oo.order_id like '_____'
and (op.product_category in ('Category1', 'Category3', 'Category5'))
and op.product_price > 100
and oc.status = 'active'
order by op.product_price desc;   



explain analyze 
select count(oc.id), sum(op.product_price) 
from opt_clients as oc
join opt_orders as oo on oc.id = oo.client_id
join opt_products as op on oo.product_id = op.product_id
where oo.order_date > '2024-06-01'
and oo.order_id like '_____'
and (op.product_category in ('Category1', 'Category3', 'Category5'))
and op.product_price > 100
and oc.status = 'active';
  



-- Optimized

create index idx_opt_products_price
    on opt_products(product_price);
  
create index idx_opt_orders_order_date
    on opt_orders(order_date);
   
create index idx_opt_product_category
	on opt_products(product_category);

create index idx_opt_client_status
	on opt_clients(status);



explain analyze
with cte as (
    select oc.id as client_id, oo.order_id, oo.order_date, oo.product_id, op.product_name, op.product_price
    from opt_clients as oc
    join opt_orders as oo on oc.id = oo.client_id
    join opt_products as op on oo.product_id = op.product_id
    where oo.order_date > '2024-06-01'
    and oo.order_id like '_____'
    and op.product_category in ('Category1', 'Category3', 'Category5')
    and op.product_price > 100
    and oc.status = 'active'
    )

select * from cte order by op.product_price desc;   



explain analyze
with cte as (
    select count(oc.id), sum(op.product_price)
    from opt_clients as oc
    join opt_orders as oo on oc.id = oo.client_id
    join opt_products as op on oo.product_id = op.product_id
    where oo.order_date > '2024-06-01'
    and oo.order_id like '_____'
    and op.product_category in ('Category1', 'Category3', 'Category5')
    and op.product_price > 100
    and oc.status = 'active'
    )

select * from cte;  
