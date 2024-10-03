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
foreign key fk_r_id (r_id) references room(r_id) on update cascade on delete cascade
);
