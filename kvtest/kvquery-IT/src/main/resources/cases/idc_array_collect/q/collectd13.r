select u.address.country,array_collect(distinct CAST(u.rating AS INTEGER)) as ratings, count(distinct u.rating) as count  from users u group by u.address.country

