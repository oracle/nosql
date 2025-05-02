#If user type is not known select as free
select u.country,array_collect([id,case when u.type IS NULL then 'FREE' else u.type end]) as memberType, count(*) as count from users u group by u.country
