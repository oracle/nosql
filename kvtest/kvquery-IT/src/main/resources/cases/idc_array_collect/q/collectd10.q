select u.country,array_collect(distinct contains(u.firstName, 'John')) as names,count(*) as count from users u group by u.country
