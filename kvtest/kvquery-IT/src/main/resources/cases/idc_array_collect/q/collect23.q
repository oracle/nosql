#for each country collect all the users and their lastnames
select u.country,array_collect([u.id,u.lastName,u.otherNames.last]) as lastNames from users u where u.country IS NOT NULL and EXISTS u.otherNames.last group by u.country, u.address.state
