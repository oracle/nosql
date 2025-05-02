select u.country,array_collect(distinct u.lastLogin) as loginTime from users u group by u.country
