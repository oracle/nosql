#collect id for timestamp>2016-11-18
select u.address.country,array_collect(u.id) as users from users u where u.lastLogin > '2016-11-18' group by u.address.country
