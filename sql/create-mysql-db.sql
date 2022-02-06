create schema if not exist sandbox;
use sandbox;

create user 'pyspark_user' identified by '123';
grant all privileges on sandbox.* to 'pyspark_user';

drop table if exists book_orders;
create table book_orders (
order_uuid varchar(100),
first_name varchar(100),
last_name varchar(100),
address varchar(300),
phone_number varchar(100),
cc_number varchar(100),
cc_expire varchar(10),
book_category varchar(500),
book_format varchar(500),
book_rating varchar(50),
kafka_offset long,
kafka_timestamp timestamp,
batch_id integer,

primary key (order_uuid),
index category_idx (book_category),
index format_idx (book_format)
)
;

drop table if exists book_orders_agg;
create table book_orders_agg (
time_window varchar(100)  null,
order_cnt integer null,
batch_id integer not null
)
;