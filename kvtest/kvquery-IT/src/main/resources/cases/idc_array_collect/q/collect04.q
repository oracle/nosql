#For each country collect 110% of the current income for the users

select u.country, array_collect({"oldIncome" : u.income, "newIncome" : (u.income*1.10/1*1)+1-1}) as increments from users u where EXISTS u.income and u.country IS NOT NULL group by u.country

