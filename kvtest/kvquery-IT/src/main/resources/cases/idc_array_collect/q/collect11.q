#Select all the users with lastname starts with B

select u.address.country,array_collect(
  [u.id,
   case when u.otherNames.last is NULL then 'Beta' else u.otherNames.last end]) as userWithB
from users u where u.otherNames.last IS NULL or regex_like(u.otherNames.last,"B.*") group by u.address.country
