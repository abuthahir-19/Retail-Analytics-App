create table if not exists pos.customers
as
select * from customers where customer_id != '"customer_id"';