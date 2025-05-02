#for each country list all the users and count of their connections

select u.country,array_collect([u.id,concat(u.firstName,u.lastName),size(u.connections)]) as connections from users u group by u.country
