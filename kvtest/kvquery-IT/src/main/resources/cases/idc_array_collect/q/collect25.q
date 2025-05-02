#Count the total number of phones numbers with areacode 339

select u.address.country, array_collect(u.address.phones[$element.areacode = 339]) as area339, count(u.address.phones[$element.areacode = 339]) as count  from users u group by u.address.country
