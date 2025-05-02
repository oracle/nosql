select u.country,array_collect(distinct u.age) as age, count(distinct u.age) as count from users u where u.age<=40 group by u.country order by country desc

