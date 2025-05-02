#for each country collect all the distinct users
select u.country,array_collect(distinct u.id) as users,count(*) as count from users u group by u.country

