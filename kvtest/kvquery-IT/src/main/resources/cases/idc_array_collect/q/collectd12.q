select u.country,array_collect(distinct u.firstName||u.lastName||'_'||age) as userId from users u group by u.country
