#For each country collect user whose raing is not set
select u.country, array_collect({"id" : u.id, "name" : u.firstName}) as userNoRating from users u where u.rating IS NULL group by u.country
