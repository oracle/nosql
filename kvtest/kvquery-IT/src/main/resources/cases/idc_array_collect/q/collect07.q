#For each country collect full name of the users

select u.country,array_collect(concat(u.firstName,u.lastName)) as fullName from users u group by u.country
