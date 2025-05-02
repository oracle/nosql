#select photos from user per country
select u.country,array_collect(distinct u.photo) as photos, count(*) as count from users u group by u.country
