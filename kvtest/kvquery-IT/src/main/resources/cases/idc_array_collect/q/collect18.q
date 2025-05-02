#select all lastlogin per country
select u.country,array_collect(u.lastLogin) as loginTime from users u group by u.country
