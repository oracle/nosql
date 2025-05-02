#concat names and age to create user id

select u.country,array_collect(u.firstName||u.lastName||'_'||age) as userId from users u group by u.country

