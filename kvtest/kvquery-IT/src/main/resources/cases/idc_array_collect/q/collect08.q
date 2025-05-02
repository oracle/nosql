#For each country collect short names of the users
select u.country,array_collect(substring(u.firstName,0,4)) as shortName from users u group by u.country
