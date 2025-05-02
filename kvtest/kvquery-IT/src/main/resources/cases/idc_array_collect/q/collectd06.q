select u.country,array_collect(distinct u.firstName) as name,count(*) as count from users u group by u.country
