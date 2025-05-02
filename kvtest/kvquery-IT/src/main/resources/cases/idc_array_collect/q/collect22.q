#For each country collect all the users who have otherNames

select u.country,array_collect(u.otherNames) as userInfo from  users u where u.otherNames IS NOT NULL group by u.country

