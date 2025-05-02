select u.country,array_collect(distinct substring(u.firstName,0,4)) as shortName,count(distinct substring(u.firstName,0,4)) as count from users u group by u.country
