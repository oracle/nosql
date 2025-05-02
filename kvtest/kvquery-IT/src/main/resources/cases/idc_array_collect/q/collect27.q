#For each user, select his/her 2 strongest connections. Group users by country
select u.address.country,array_collect({"id": u.id, "strong":u.connections[0:2]}) as strongestConnection from users u group by u.address.country
