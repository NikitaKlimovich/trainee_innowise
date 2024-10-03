select r_name, student.r_id, count(s_id) as stud_count from student
join room on student.r_id = room.r_id
group by r_id;

select r_name, room.r_id, cast(round(avg(year(curdate()) - year(s_birthday)),2) as float) as avg_age from student
join room on student.r_id = room.r_id
group by r_id
order by avg_age desc
limit 5;

select r_name, room.r_id, max(year(s_birthday))-min(year(s_birthday)) as diff from student
join room on student.r_id = room.r_id
group by r_id
order by diff desc
limit 5;

select r_name, room.r_id from room
join
(select q1.r_id from
(select distinct r_id from student
where s_sex = 'M') as q1
join
(select distinct r_id from student
where s_sex = 'F') as q2
on q1.r_id = q2.r_id) as q12 on room.r_id = q12.r_id;
