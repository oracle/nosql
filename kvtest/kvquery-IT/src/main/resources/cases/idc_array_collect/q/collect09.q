select u.country,array_collect(upper(u.firstName)) as name from users u group by u.country
