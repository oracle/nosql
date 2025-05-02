select u.country,array_collect(distinct u.otherNames) as userInfo,count(*) as count from  users u where u.otherNames IS NOT NULL group by u.country

