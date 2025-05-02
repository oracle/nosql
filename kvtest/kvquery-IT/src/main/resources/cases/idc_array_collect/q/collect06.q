#For each country collect names of the users

select u.country,array_collect(u.firstName) as name,count(*) as count from users u group by u.country
