select u.country,array_collect(distinct upper(u.type)) as userType,count(*) as count from users u group by u.country
