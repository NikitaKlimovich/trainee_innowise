-- Вывести количество фильмов в каждой категории, отсортировать по убыванию
select c.name, count(film_id) as film_cnt 
from public.film f
join public.film_category fc using (film_id)
join public.category c using (category_id)
group by c.name
order by film_cnt desc;

-- Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию
select a.first_name, a.last_name, sum(rental_duration) as rent_dur
from public.film f
join public.film_actor fa using (film_id)
join public.actor a using (actor_id)
group by a.first_name, a.last_name
order by rent_dur desc
limit 10;


-- Вывести категорию фильмов, на которую потратили больше всего денег
select c.name, sum(replacement_cost) as ctgr_cost 
from public.film f
join public.film_category fc using (film_id)
join public.category c using (category_id)
group by c.name
order by ctgr_cost desc
limit 1;


-- Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
select f.title 
from public.film f
left join public.inventory i using (film_id)
where i.film_id is NULL;


-- Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
with actor_film_cnt as
(
	select a.first_name, a.last_name, count(*) as film_cnt
	from public.film f
	join public.film_category fc using (film_id)
	join public.film_actor fa using (film_id)
	join public.category c using (category_id)
	join public.actor a using (actor_id)
	where c.name = 'Children'
	group by a.first_name, a.last_name
)
select max_cnt.first_name, max_cnt.last_name, max_cnt.film_cnt 
from
(	select afc.first_name, afc.last_name, afc.film_cnt, dense_rank() over(order by film_cnt desc) as rnk
	from actor_film_cnt afc
) max_cnt
where max_cnt.rnk<=3;



-- Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
with customer_city as
(
	select cu.first_name, cu.last_name, cu.active, ci.city  
	from public.customer cu
	join public.address a using(address_id)
	join public.city ci using (city_id)
)
select cc.city, cc.active as is_active, count(*) as cust_cnt
from customer_city cc
group by cc.city, cc.active
order by cc.active desc, cust_cnt desc;


-- Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
with category_city as
(
	select ci.city, ca.name, f.rental_duration
	from public.film f
	join public.film_category f_c using (film_id)
	join public.inventory i using (film_id)
	join public.rental r using (inventory_id)
	join public.customer cu using (customer_id)
	join public.address a using (address_id)
	join public.city ci using (city_id)
	join public.category ca using (category_id)
	where ci.city like 'a%' or ci.city like 'A%' or ci.city like '%-%'
),
category_rental_duration as
(
	select name, sum(rental_duration) as rent_sum, 'a.. or A..' as template_city
	from category_city cc
	where cc.city like 'a%' or cc.city like 'A%'
	group by name
	union
	select name, sum(rental_duration) as rent_sum, '..-..' as template_city
	from category_city cc_1
	where cc_1.city like '%-%'
	group by name
)
select max_rent.name, max_rent.rent_sum, max_rent.template_city
from
(
	select name, rent_sum, max(rent_sum) over(partition by template_city) as max_rent_sum, template_city
	from category_rental_duration
) max_rent
where max_rent.rent_sum = max_rent.max_rent_sum
;


