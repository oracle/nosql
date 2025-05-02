#For each country collect all the ages <= 40
select u.country, array_collect(u.age) as age, count(u.age) as count
from users u
where u.age <= 40
group by u.country
order by country desc
