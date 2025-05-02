#Select the strongest connection of the user with id 10.
select u.address.country,array_collect({"id": u.id, "strong":u.connections[0]}) as strongestConnection from users u group by u.address.country
