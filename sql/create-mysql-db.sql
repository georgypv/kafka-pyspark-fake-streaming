create schema sandbox;
use sandbox;

create user 'pyspark_user' identified by '123';
grant all privileges on sandbox.* to 'pyspark_user';

drop table fake_stream_1;
create table fake_stream_1 (
order_uuid varchar(100),
cafe_name varchar(100),
first_name varchar(100),
last_name varchar(100),
address varchar(300),
phone_number varchar(100),
cc_number varchar(100),
cc_expire varchar(10),
dishes varchar(500),
beverages varchar(500),
kafka_offset long,
kafka_timestamp timestamp,
batchId integer,

primary key (order_uuid),
index cafe_idx (cafe_name)
)
;