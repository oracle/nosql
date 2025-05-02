select u.country, array_collect(distinct {"oldIncome" : u.income, "newIncome" : (u.income*1.10/1*1)+1-1}) as increments, count(*) as count from users u where EXISTS u.income and u.country IS NOT NULL group by u.country

