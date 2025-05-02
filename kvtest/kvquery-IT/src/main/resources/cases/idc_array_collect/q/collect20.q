#For each country list all the users and their connections
select u.country,array_collect({"id" : u.id, "connections" : u.connections[]}) as users from users u where u.country IS NOT NULL  group by country order by country
