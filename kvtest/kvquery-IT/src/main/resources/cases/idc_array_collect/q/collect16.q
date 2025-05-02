#collect all paid users per country
select u.country,array_collect([u.id,u.firstName,upper(u.type)]) as paidUsers from users u where u.type=lower('PAID') group by u.country
