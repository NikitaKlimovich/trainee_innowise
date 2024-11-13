create database rs_db;
use rs_db;

create table if not exists room (
r_id int primary key,
r_name varchar(15)
);

create table if not exists student (
s_id int primary key,
s_name varchar(40),
s_birthday date,
s_sex char(1),
r_id int,
foreign key fk_r_id (r_id) references room(r_id) on delete cascade 
);

create index idx_r_name on room (r_name);
create index idx_s_birthday on student (s_birthday);