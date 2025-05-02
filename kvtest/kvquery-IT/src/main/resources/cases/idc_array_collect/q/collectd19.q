#Add 5 minutes to lastLogin
select u.country,array_collect(distinct timestamp_add(u.lastLogin,'5 minutes')) as updatedLogin from users u group by u.country
