#For each country select user id and their ratings
select u.address.country,array_collect([u.id,u.rating]) as ratings from users u group by u.address.country

