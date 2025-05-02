#Among the 10 strongest connections of each user, select the ones with id > 100

select u.address.country, array_collect({"id":u.id,"strongConnections" : u.connections[$element>100 AND $pos<5]}) as interestingConnections from users u group by u.address.country
