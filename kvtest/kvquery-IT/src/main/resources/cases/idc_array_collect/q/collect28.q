#For each user, select his/her 2 weakest connections.
select u.address.country,array_collect({"id": u.id, "strong":u.connections[size($)-2]}) as strongestConnection from users u group by u.address.country
