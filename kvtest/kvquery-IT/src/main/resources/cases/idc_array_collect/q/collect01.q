#For each country collect all the user ids
select u.country,array_collect(u.id) as users,count(*) as count from users u group by u.country
